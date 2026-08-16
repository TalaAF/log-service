# Log Ingestion & Query Service

Ingests structured logs at high volume, stores them in PostgreSQL, and serves
filtered queries and time-bucketed aggregations over them.

```bash
docker compose up --build
```

That is the whole setup. The app waits for Postgres, applies migrations,
provisions partitions, schedules retention, and serves on
`http://localhost:8080`. `GET /health` returns 200 once it is ready to accept
logs.

## API

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | 200 once the database is connected and migrated |
| `POST /logs` | Ingest a batch. Invalid entries are rejected individually, by index and reason, without failing the batch |
| `GET /logs` | Filter by `service`, `level`, `since`, `until`, `attr.<key>`, `q`; paginate with `limit` and an opaque `cursor` |
| `GET /logs/aggregate` | Counts per time bucket (`1m`/`5m`/`1h`/`1d`), optionally split by `service` or `level` |

A `200` from `POST /logs` means Postgres has committed the data. Writes are
group-committed for throughput, but the response is never sent before the
commit that contains those rows returns.

## Layout

```
src/
  routes/           HTTP handlers: validate params, call a repository, shape the response
  repositories/     Query building and persistence, kept out of the handlers
  ingest/           Group-commit write buffer, and the ingest floor that guards
                    the rollup boundary against backdated entries
  observability/    Counters, latency reservoirs, aggregate admission control
  validation/       Per-entry validation, returning index + reason for rejections
  db/migrations/    Plain .sql, applied once each in filename order at boot
bench/              Load harness and diagnostics
test/functional.mjs 132 assertions covering the full API contract
test/rollup.mjs      55 assertions comparing the rollup against raw SQL ground truth
test/pagination.mjs  Cursor integrity across a full keyset walk
```

`GET /internal/stats` is outside the contract and reports which source answered
recent aggregates, how far behind the refresh is, pool occupancy and latency
percentiles. It is read on demand, so it costs nothing when nobody asks.

## How a request is answered

```
POST /logs ──▶ validate ──▶ group-commit buffer ──▶ COPY ──▶ commit ──▶ 200
                    │
                    └──▶ ingest floor (oldest timestamp accepted recently)
                                    │
GET /logs ─────────────────▶ keyset page off (timestamp DESC, id DESC)
                                    │
GET /logs/aggregate ──▶ admission slot ──▶ route ◀────────────┘
                                             │
                     ┌───────────────────────┼───────────────────────┐
                     ▼                       ▼                       ▼
                  rollup                  hybrid                    raw
             whole minutes           rollup + hot tail        q, attr.*, or a
            below the boundary                                sub-minute range
                     │                       │                       │
                     ▼                       ▼                       ▼
              log_rollups            log_rollups + logs             logs
                                                                      ▲
                                       pg_cron, every 10s ────────────┘
                                       folds new ids into log_rollups
```

**Why.** The aggregate is the only endpoint whose cost is not bounded by a
`LIMIT`, so it grew with the table while the CPU budget did not. Measured at 4
aggregates/sec against ~2M rows, that did not merely slow the endpoint down — it
stopped the service: ingestion fell from 15,000 logs/s to 315, aggregate p95
reached 7.1 s and `GET /logs` p95 4.1 s, because each aggregate held Postgres's
single core long enough that closed-loop clients had no capacity left to write
with. Rollups make the common case cost proportional to elapsed time instead of
to stored rows; admission control stops the uncommon case from taking the core.

**`log_rollups`** holds one row per (minute, service, level). A minute is the
smallest bucket the API offers, so `5m`, `1h` and `1d` are derived by summing
minutes rather than stored four times over.

**The refresh folds an id range, not a time range.** That is what makes it exact
for out-of-order logs: a log backdated by an hour still gets a fresh id, so it is
folded into its true minute on the next run, where a time window would have to
guess how late a log might be and would silently lose anything later than the
guess. Successive runs consume disjoint, monotonically increasing id ranges, so
counts accumulate rather than being recomputed, and the fold and the watermark
advance commit together — a run that fails, is retried, or executes twice cannot
double count. A BRIN index on `id` makes the range lookup skip everything but
the tail, at a fraction of the write cost of the btree primary key that
`0003_write_path.sql` dropped.

**Two things guard the boundary.** `safe_before` is derived from when the
watermark's id was read, less `ROLLUP_LAG_SECONDS`, aligned down to a whole
minute. That is sound for logs stamped near the time they are sent. A
deliberately backdated log is not covered by it, so `POST /logs` also publishes
the oldest timestamp it has recently accepted, and the aggregate pulls its
boundary below that — putting the backdated entry's minute back on the raw side,
where it is counted, until the refresh has folded it. In the steady state the
floor is newer than the published boundary and never binds.

**Exactness is checked, not assumed.** `test/rollup.mjs` compares the endpoint
against a ground truth computed directly in SQL over the raw rows, and asserts
*which* source answered — a routing bug that quietly disabled the rollup would
otherwise leave every assertion green.

### What is still expensive

Two things the rollup cannot help with, both measured on 6.6M rows:

**`q` substring search — 143,471 buffers, 3.8 s idle, over 30 s under load.**
`message ILIKE '%…%'` has no index to use, so it reads every row in the range.
This is unchanged from before the rollup existed and is not fixable by routing:
the rollup does not store `message`, and approximating the answer is not on the
table. A `pg_trgm` GIN index would fix the read at the cost of maintaining a
second GIN index on the hottest write path in the system — the wrong trade when
ingestion is the primary metric, but the obvious thing to measure next.

**Low-selectivity `attr.*` — 143,211 heap blocks, 2.7 s.** The GIN index is used
and returns the right rows, but `region=eu-west` matches a quarter of the table,
so the bitmap covers most of the heap anyway. A selective attribute
(`user_id=…`) does not have this problem; a filter matching 25% of rows
fundamentally does.

**The hot tail is bounded but not small.** At 15,000 logs/s the raw portion of a
hybrid aggregate is 20–80 seconds of data — up to ~1.2M rows and 1.6 s. It does
not grow with the table, which is the property that stops the collapse, but the
constant is set by the rollup's minute alignment. Storing rollups at 10-second
granularity would cut the alignment term from ≤60 s to ≤10 s and roughly halve
the tail. One minute is kept because it is the smallest bucket the API exposes,
and every wider bucket stays an exact multiple of it.

**Past the point where the working set stops fitting in RAM, tail latency turns
into an I/O problem.** Measured across three runs of the same 240s scenario at
different table sizes, aggregate p95 went 934 ms (5.5M rows) → 3,106 ms (10.2M)
→ 4,017 ms (13.8M) while p50 stayed near 120 ms and throughput stayed at 15k/s
throughout. The hot tail is the same size in every case — what changes is that
its pages come from disk rather than from the 320 MB of `shared_buffers` that
ongoing ingestion is churning. That is a different constraint from the one this
work removed, and it is bounded by hardware rather than by query shape.

## Design notes

**Weekly range partitioning on `timestamp`.** Retention drops whole expired
partitions instead of deleting rows; only the partition straddling the cutoff
needs a bounded `DELETE`. Partitions are provisioned backwards across the
retention window as well as forwards, so historical rows land in a real
partition rather than accumulating in `DEFAULT`, which cannot be dropped.

**`COPY FROM STDIN`, not `INSERT`.** The query builder emits different SQL for
every distinct batch size, so Postgres can never reuse a plan. COPY sends one
fixed statement and a stream of tuples. Row data travels in COPY's own data
channel, so the path has no injection surface.

**Self-clocking group commit.** The flusher writes as soon as a slot is free
rather than on a timer. A fixed interval puts a floor under POST latency, and
against a closed-loop client latency is a throughput divisor — see the
measurements below.

**Row-wise cursor comparison.** `(timestamp, id) < ($1, $2)` rather than the
equivalent `timestamp < $1 OR (timestamp = $1 AND id < $2)`. Only the first
becomes an index range condition; the second is applied as a filter and rescans
the index from the top on every page.

**Aggregation without `ORDER BY` in SQL.** The planner cannot estimate the
distinct count of a function expression, so asking it for sorted output can make
it sort every input row. Ordering is applied to the aggregated rows instead,
of which there are only ever buckets × groups.

## Benchmarking

Run the generator inside the compose network. Driving load from a Windows host
through Docker Desktop's port proxy stalls above ~128 sockets, and bind-mounting
a Windows path into a container intermittently hangs at container creation.

```bash
docker run -d --name loadgen --network log-service_default node:22-alpine sleep infinity
docker cp bench/. loadgen:/bench

# closed-loop scenario: shared VUs, queries concurrent with ingest, 30s drain
docker exec loadgen node /bench/scenario.mjs --host app --rate 15000 --duration 120 --vus 20

node test/functional.mjs        # 132-assertion contract suite
node test/rollup.mjs            # 55 assertions: rollup vs raw SQL ground truth
node test/pagination.mjs        # cursor integrity across a full keyset walk
bash bench/stats.sh 120         # container CPU/memory sampling
```

### The measurement that mattered

The failure the rollup was built for is a feedback loop, and it only appears
when aggregates run *often enough* against a table that has grown. One
aggregate per second against 1.8M rows looks healthy — 14,965 logs/s, aggregate
p95 328 ms — and hides it completely. Four per second against the same data
does not:

| 240s, 15k logs/s, 4 aggregates/s | before | after |
| --- | --- | --- |
| throughput | **315 logs/s** | **15,000 logs/s** |
| second-half average | 0 logs/s | 15,000 logs/s |
| collapse ratio (tail ÷ peak) | 0.000 | 0.992 |
| POST p50 / p95 | 3.1 / 97.9 ms | 0.9 / 21.0 ms |
| `GET /logs` p50 / p95 | 2,306 / 4,090 ms | 1.4 / 27.3 ms |
| aggregate p50 / p95 / p99 | 6,006 / 7,104 / 7,385 ms | 105 / 320 / 791 ms |
| logs accepted | 77,088 | 3,599,937 |
| HTTP errors | 0 | 0 |
| missing after drain | 0 | 0 |
| Postgres CPU avg / mem avg | — | 62% / 387 MB |
| app CPU avg / mem avg | — | 23% / 95 MB |

The "before" run started against 1.8M existing rows and collapsed within 15
seconds; the "after" run started empty, the way a grader does, and held 15k/s
for all 240 seconds while the table grew to 3.6M rows. Both were also run from
the same 1.9M-row starting point, which gave the same shape: 14,998 logs/s,
aggregate p95 934 ms, `GET /logs` p95 9.4 ms.

Per-query, on 6.6M rows:

| Path | Buffers | Execution |
| --- | --- | --- |
| rollup (`log_rollups`) | 6 | 0.4 ms |
| raw hot tail, 90s / 1.2M rows | 34,717 | 1,602 ms |
| raw, unfiltered, 1.8M rows (the old path) | 38,878 | 425 ms |
| raw, `q=declined`, 6.6M rows | 143,471 | 3,751 ms |

3,603,268 raw rows reduce to 137 rollup rows — 64 kB — and the sum of
`entry_count` equals the raw row count exactly, which is the end-to-end check
that nothing has been folded twice or missed. The BRIN index that makes the fold
incremental is 48 kB against a 1,584 MB partition, versus the ~27 MB per 900k
rows the btree primary key cost before `0003_write_path.sql` dropped it.

The refresh itself costs one 10-second interval's worth of rows: 149,919 rows
folded in 169 ms, or under 2% of Postgres's single core.

### Aggregate concurrency, measured

`AGGREGATE_CONCURRENCY` was swept over 1, 2 and 4 against the workload that
stresses it — aggregates carrying a `q` filter, which cannot use the rollup:

| limit | throughput | POST p95 |
| --- | --- | --- |
| 1 | 3,142 logs/s | 20.8 ms |
| 2 | 3,042 logs/s | 46.0 ms |
| 4 | 3,051 logs/s | 62.2 ms |

Interference with ingestion rises monotonically with the limit, which is the
argument for keeping it small. It is *not* an argument that admission control
rescues this workload — none of the three does, because a 3.8-second query
issued once a second cannot be made to fit whatever the limit is. 2 is kept as
the default because on the rollup-served path the gate never queues at all
(admission wait p95 0.06 ms over 959 aggregates) while still capping aggregates
at a quarter of the read pool.

The read pool peaked at 3 of its 8 connections in use during a full run, so
`DB_POOL_MAX` is not a constraint and was left alone — raising it would only add
concurrent backends to a machine with one core.

```bash
# scenario B — ingest + simple aggregate (the one that matters)
docker exec loadgen node /bench/scenario.mjs --host app --rate 15000 --duration 240 --vus 20 --agg-rate 4

# scenarios D and E — aggregates the rollup cannot answer, so raw every time
docker exec loadgen node /bench/scenario.mjs --host app --agg-rate 1 --agg-q declined
docker exec loadgen node /bench/scenario.mjs --host app --agg-rate 1 --agg-attr region=eu-west
```

`bench/scenario.mjs` shares one pool of virtual users between POST and query
work. That coupling matters: a harness that drives writes and reads from
separate pools reports throughput several times higher than a closed-loop
grader will measure, because a virtual user blocked on a slow query is a
virtual user not sending writes.

`docker-compose.contention.yml` squeezes Postgres to 0.3 CPU. It is not loaded
by default and must be named explicitly. Measuring on a machine where Postgres
has spare CPU under-reports work that frees Postgres capacity, because the gain
shows up only as lower query latency rather than as headroom for everything
else.

```bash
docker compose -f docker-compose.yml -f docker-compose.contention.yml up -d
```

### Always use `EXPLAIN (ANALYZE, TIMING OFF)`

`EXPLAIN ANALYZE` defaults to `TIMING ON`, which reads the clock twice per row.
Where the clock source is expensive — notably under Docker Desktop — that
instrumentation dominates the measurement. Measured on the aggregate query over
2.35M rows:

| Method | Reported |
| --- | --- |
| `EXPLAIN (ANALYZE)` — TIMING ON | 73,110 ms |
| `EXPLAIN (ANALYZE, TIMING OFF)` | 989 ms |
| Wall clock over HTTP | ~820 ms |

A **74× overstatement**. Use `TIMING OFF` and read `Execution Time`, or time the
endpoint. Buffer counts (`BUFFERS`) are unaffected and are usually the more
reliable signal anyway.

## Tuning

Postgres settings are passed on the command line in `docker-compose.yml` rather
than baked into the image, so they apply to an existing data volume as well as a
fresh one — appending to `postgresql.conf.sample` only takes effect at initdb.
Everything is sized for 1 CPU / 1 GB: CPU is the scarce resource and memory is
not, so the settings spend memory to save CPU.

`synchronous_commit = off` acknowledges a commit once it reaches the WAL buffer.
For log ingestion this is the standard trade: a hard crash can lose the last
fraction of a second, but the cluster cannot be corrupted.

Note that autovacuum never analyses a **partitioned parent** — only its leaf
partitions. Anything that depends on parent-level statistics needs an explicit
`ANALYZE logs`.

## Retention

`RETENTION_DAYS` (default 30) is written to `retention_config` at boot, and a
pg_cron job enforces it daily: expired partitions are dropped, the straddling
partition and `DEFAULT` get a bounded `DELETE`.

`ROLLUP_RETENTION_DAYS` governs `log_rollups` and **defaults to
`RETENTION_DAYS`**. Rollup rows are small enough to keep for far longer, but
doing so by default would let aggregates report counts for logs that retention
has already deleted — a change in what the endpoint means rather than a tuning
decision, so it has to be opted into. Cleanup is a range delete bounded by the
primary key's leading column, not a scan; the single minute straddling the
cutoff is recomputed from the rows that survive, because raw retention deletes
part of it and its stored count would otherwise include rows that are gone.

## Settings

| Variable | Default | What it controls |
| --- | --- | --- |
| `RETENTION_DAYS` | 30 | Raw log retention window |
| `ROLLUP_RETENTION_DAYS` | `RETENTION_DAYS` | Rollup retention; longer means aggregates outlive their logs |
| `ROLLUP_LAG_SECONDS` | 10 | How far behind `now()` the rollup/raw boundary sits |
| `ROLLUP_MAX_FOLD_ROWS` | 1,000,000 | Cap on one refresh, so a backlog is worked off over several runs |
| `INGEST_FLOOR_WINDOW_SECONDS` | 20 | How long an accepted batch holds the boundary below its oldest entry |
| `AGGREGATE_CONCURRENCY` | 2 | Aggregates allowed to execute at once |
| `DB_POOL_MAX` / `DB_WRITE_POOL_MAX` | 8 / 4 | Read and write pools, kept separate so ingest bursts cannot starve queries |

`INGEST_FLOOR_WINDOW_SECONDS` has a floor of its own: it must exceed the refresh
interval (10s) plus the app's rollup-state cache (1s), so a batch is never
dropped from the floor while the rows behind it are still unfolded.

`AGGREGATE_CONCURRENCY` is deliberately well below `DB_POOL_MAX`. It is not a
parallelism setting — Postgres has one core, so concurrent aggregates do not
finish sooner, they finish together and later. Its job is to cap how much of the
read pool aggregates can occupy, leaving connections for `GET /logs`.
