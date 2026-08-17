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
            whole 10s buckets        rollup + hot tail        q, attr.*, or a
            below the boundary                              sub-bucket range
                     │                       │                       │
                     ▼                       ▼                       ▼
              log_rollups            log_rollups + logs             logs
                                                                      ▲
                                       pg_cron, every 2s ─────────────┘
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

**`log_rollups`** holds one row per (10 seconds, service, level). Every bucket
width the API offers is a whole multiple of ten seconds, so `1m`, `5m`, `1h` and
`1d` are derived by summing stored rows rather than stored four times over.

Ten seconds rather than a minute because the stored width sets a floor under how
close the rollup/raw boundary can get to now(): the boundary has to land on a
stored bucket edge. That floor is what decides the size of the hot tail, and the
hot tail is bounded in *time* but not in *rows*, so its cost scales with the
ingest rate. At a minute it was 20-80 seconds of raw data, which is 600k-2.4M
rows at 30,000 logs/s — enough to reopen the collapse the rollup exists to
prevent. See the ramp measurements below.

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
watermark's id was read, less `ROLLUP_LAG_SECONDS`, aligned down to a stored
bucket edge. That is sound for logs stamped near the time they are sent. A
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

**`q` substring search is `ILIKE`, not I/O and not aggregation.** Decomposed on
one fixed 1.8M-row dataset, same plan and same ~38,900 buffers throughout:

| predicate | time |
| --- | --- |
| seq scan + `service =` | 170 ms |
| + `LIKE '%declined%'` (case-sensitive) | 252 ms |
| + `ILIKE '%declined%'` | **922 ms** |
| `GROUP BY` bucket, service, no `q` | 403 ms |

`ILIKE` costs 3.7x what `LIKE` does on identical rows: the database collation is
`en_US.utf8`, so case folding is locale-aware and runs per row. Aggregation adds
~233 ms and I/O adds nothing until the working set outgrows `shared_buffers`.
Cost is linear in rows scanned at roughly 0.6 us/row:

| rows | table | `q` time | disk reads |
| --- | --- | --- | --- |
| 1.35M | 332 MB | 671 ms | 1,780 |
| 2.40M | 580 MB | 1,562 ms | 26,789 |
| 3.45M | 848 MB | 2,197 ms | 48,533 |
| 4.50M | 1,091 MB | 3,174 ms | 71,435 |

`shared hit` stays flat at ~26,000 across all four; every row past the 320 MB
cache becomes a physical read, which is the ~40% per-row premium between the
first row and the last. At the size a 120s run at 15k/s actually reaches, `q`
p95 under full concurrent load is **723 ms**, not the seconds it costs on a
database that has accumulated several runs' worth of rows.

Three ways out were measured, and all three were rejected:

*A cheaper predicate.* `(message COLLATE "C") ILIKE` is 2.2x faster (415 ms
against 912 ms) and returns identical counts on this data — because this data is
ASCII. C collation folds only `A-Z`, so `CAFÉ` would stop matching `café`. That
is a silent change in what `q` means, for a constant factor.

*A separate search table.* The decomposition rules it out before it is built:
the cost is `ILIKE` CPU, which a narrower table does not reduce. It would save
the I/O term — the smaller of the two — and pay for a second copy of every
message.

*`pg_trgm`.* Re-tested from an empty database after the rollup work freed
Postgres CPU (62% -> 34%), on the theory that its write cost might now be
affordable. It is not:

| Load scenario | without | with GIN(message gin_trgm_ops) |
| --- | --- | --- |
| throughput | 14,993 logs/s | 14,015 logs/s |
| POST p95 | 5.9 ms | 66.0 ms |
| simple aggregate p95 | 88 ms | 1,084 ms |
| read-back | 105k rows/s | 42k rows/s |
| eventual consistency | passed | **FAILED**, 428k missing |
| Postgres CPU avg / max | 34.5% / 79% | 52.1% / 109% |
| index size | — | 112 MB |

And it barely pays even on the read side at this scale: `q=declined` 960 ms
(the term matches 12.5% of rows, so the bitmap covers the heap anyway), `q=ab`
2,967 ms — *worse*, because a two-character pattern yields no usable trigram and
GIN returns every row to be rechecked. Only a selective term wins (54 ms). The
index buys one query shape and costs the whole system, including the
eventual-consistency check it pushes over its budget.

So `q` is contained rather than accelerated: `AGGREGATE_SCAN_CONCURRENCY` holds
it to one concurrent scan. Measured from identical empty databases at 15k logs/s
with `q` and `GET /logs` both running, limits of 1, 2 and 4 are indistinguishable
(14,896-14,897 logs/s, `q` p95 708-723 ms, zero errors, EC passing in all three),
so the limit only matters once the table is large enough to saturate the CPU —
which is exactly when holding it to one matters most.

**Low-selectivity `attr.*` — 143,211 heap blocks, 2.7 s.** The GIN index is used
and returns the right rows, but `region=eu-west` matches a quarter of the table,
so the bitmap covers most of the heap anyway. A selective attribute
(`user_id=…`) does not have this problem; a filter matching 25% of rows
fundamentally does.

**The read-back walk is the other ceiling, and ingesting well makes it harder.**
The eventual-consistency check walks every accepted row through the cursor
inside a fixed budget, so the faster ingestion is, the more there is to read
back.

It is O(N) in depth — page latency at the end of a 1.9M-row walk is 0.79 ms
against 1.37 ms at the start, so the old quadratic cursor is genuinely gone —
and the limit is per-request overhead rather than per-row work. Sweeping the
page size fits `page_ms = 0.48 + 0.0024 x rows`:

| page size | page p50 | rows/s |
| --- | --- | --- |
| 1 | 0.48 ms | 1,623 |
| 100 | 0.86 ms | 84,575 |
| 1000 | 2.92 ms | 180,254 |

At page size 100 the fixed 0.48 ms is 56% of the page, so throughput is set by
how many requests the walk makes, which is the verifier's choice and not ours.
Of that fixed cost, roughly 0.27 ms is the database round trip and 0.21 ms is
HTTP and the application. Postgres attributes much of its share to planning
across eleven partitions — 0.816 ms planning against 0.168 ms execution on a
cold catalog — but plan caching already absorbs most of it: a named prepared
statement, measured returning byte-identical rows, is only 1.12x faster at page
size 100 and no faster at 1000. That is not worth restructuring the most
correctness-sensitive query in the system for.

In practice the walk sustains ~105,000 rows/s during a real drain, which clears
the 1.8M rows of a 120s run at 15k/s in 20 s of a 30 s budget. It does not clear
the 5M rows the 45k ramp produces — succeeding harder at ingestion is what makes
that check fail, and no micro-optimisation closes a 1.6x gap.

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
when aggregates run *often enough* against a table that has grown. One aggregate
per second against 1.8M rows looks healthy — 14,965 logs/s, aggregate p95
328 ms — and hides it completely. Four per second against the same data does
not. Against the official benchmark's own load scenario (15k logs/s, 120s,
aggregates concurrent, read-back walked at page size 100):

| Load scenario | official baseline | local before | local after |
| --- | --- | --- | --- |
| throughput | 2,927 logs/s | 315 logs/s | **14,993 logs/s** |
| POST p50 / p95 | — / 156.9 ms | 3.1 / 97.9 ms | 0.9 / 5.9 ms |
| `GET /logs` p95 | — | 4,090 ms | 40.6 ms |
| aggregate p50 / p95 / p99 | — / 5,500 / — ms | 6,006 / 7,104 / 7,385 ms | 44 / 88 / 139 ms |
| overall p95 | 5,000 ms | — | 8.8 ms |
| accepted | 351.2K | 77K | 1,800,018 |
| visible after drain | 98K | — | 1,800,018 |
| missing | 253.2K | 0 | **0** |
| eventual consistency | FAILED | — | PASSED (20.2s of 30s) |
| response shape | FAILED | — | PASSED (600 checks) |
| HTTP errors | 0% | 0 | 0 |
| Postgres CPU avg / max | 76.2% / 100.8% | — | 34.5% / 79.3% |
| Postgres memory | — | — | 303 MB avg |
| app CPU avg / max | 7.6% / 50.8% | — | 24.5% / 35.8% |

### The ramp is where the rollup granularity showed up

Stress and breakpoint push past the nominal rate. Storing rollups by the minute
survived 22.5k/s and fell over at 30k, because the hot tail is bounded in time
but not in rows — a 20-80 second tail is 600k-2.4M rows at 30k/s, which is
enough to take the core back and reopen the loop. Ten-second buckets cut the
alignment term from ≤60s to ≤10s:

| offered | 1-minute buckets | 10-second buckets |
| --- | --- | --- |
| 15,000 logs/s | 15,001 | 15,001 |
| 22,500 logs/s | 22,503 | 22,483 |
| 30,000 logs/s | **6,870** | **30,000** |
| 45,000 logs/s | 13,699 | **44,317** |
| aggregate p50 / p95 | 3,407 / 16,009 ms | 82 / 818 ms |
| Postgres CPU avg | 80.0% | 59.7% |

The "after" run also started from a *larger* table (9.9M rows against 5.4M), so
the comparison understates the change. The service now tracks three times the
nominal benchmark rate instead of collapsing at twice it.

Per-query, on 6.6M rows:

| Path | Buffers | Execution |
| --- | --- | --- |
| rollup (`log_rollups`) | 6 | 0.4 ms |
| raw hot tail, 90s / 1.2M rows | 34,717 | 1,602 ms |
| raw, unfiltered, 1.8M rows (the old path) | 38,878 | 425 ms |
| raw, `q=declined`, 6.6M rows | 143,471 | 3,751 ms |

The rollup stays a rounding error against the raw table — 9,893,561 raw rows
folded into 1,521 stored rows, with `sum(entry_count)` equal to the raw row
count exactly, which is the end-to-end check that nothing was folded twice or
missed. The BRIN index that makes the fold incremental is 48 kB against a
1,584 MB partition, versus the ~27 MB per 900k rows the btree primary key cost
before `0003_write_path.sql` dropped it. A refresh folds one interval's rows:
150k rows in 169 ms, under 4% of the core at 30k logs/s.

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
| `INGEST_FLOOR_WINDOW_SECONDS` | 12 | How long an accepted batch holds the boundary below its oldest entry |
| `AGGREGATE_CONCURRENCY` | 2 | Bounded (rollup/hybrid) aggregates at once |
| `AGGREGATE_SCAN_CONCURRENCY` | 1 | Unbounded (`q`/`attr.*`) aggregates at once |
| `DB_POOL_MAX` / `AGGREGATE_DB_POOL_MAX` / `DB_WRITE_POOL_MAX` | 8 / 1 / 4 | Total read connections / aggregate-reserved share / write connections |

`INGEST_FLOOR_WINDOW_SECONDS` has a floor of its own: it must exceed the refresh
interval (5s) plus the app's rollup-state cache (1s), so a batch is never
dropped from the floor while the rows behind it are still unfolded. It is also a
floor under how close the boundary can get to now(), and every second of it is
another *ingest-rate* rows a hybrid aggregate must read — which is why it is not
simply set generously.

Admission control has two lanes, because aggregates have two cost classes.
A rollup or hybrid request is bounded by the refresh boundary and costs tens of
milliseconds; one carrying `q` or `attr.*` cannot use the rollup at all and
reads every raw row in the requested range. Sharing one limit would force a
choice between throttling the cheap queries needlessly and letting the expensive
ones run several at a time, so `AGGREGATE_CONCURRENCY` paces the bounded class
and `AGGREGATE_SCAN_CONCURRENCY` holds the unbounded one to a single scan.

Neither is a parallelism setting — Postgres has one core, so concurrent
aggregates do not finish sooner, they finish together and later. Both are kept
well below `DB_POOL_MAX` so paged reads always have connections left.
