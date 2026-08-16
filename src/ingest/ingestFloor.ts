/**
 * The oldest timestamp this process has recently accepted.
 *
 * The rollup refresh publishes `safe_before`, below which every row is supposed
 * to be folded already. It derives that boundary from when it read the id
 * watermark, which is sound for logs stamped at roughly the time they are sent
 * — but not for a log deliberately backdated further than the configured lag.
 * Such a log lands under a boundary that has already moved past it, and until
 * the next refresh folds it the aggregate would read its minute from the rollup
 * and not count it. Measured with the boundary pushed to now(): 250 backdated
 * entries were accepted, were readable through GET /logs, and were missing from
 * GET /logs/aggregate.
 *
 * Nothing in the database can predict that, but this process does not have to
 * predict it — POST /logs is the only way rows are written, so it already knows.
 * Every accepted batch contributes its oldest timestamp here, and the aggregate
 * pulls its boundary down to match. A backdated log therefore moves the
 * boundary below itself the moment it is accepted, so its minute is read from
 * the raw table until the refresh has folded it.
 *
 * Samples are held for a window comfortably longer than it takes a row to be
 * folded and the new boundary to be observed. In the steady state, where logs
 * are stamped at send time, the oldest sample is about one window old and the
 * boundary is already further back than that, so this never binds and costs
 * nothing but a comparison.
 *
 * The limit of the mechanism: rows written out of band, by something other than
 * this service, are not seen here. Those are covered by the refresh itself,
 * which folds them by id whatever their timestamp, one refresh interval later.
 */

function intFromEnv(name: string, fallback: number): number {
  const parsed = Number(process.env[name]);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

/**
 * How long an accepted batch keeps holding the boundary down.
 *
 * Bounded below by how long a row can stay unfolded: the refresh interval (5s)
 * plus the second the application caches the published boundary for, so about
 * six. Dropping a sample earlier than that would release the boundary over rows
 * that are still missing from the rollup.
 *
 * Bounded above by cost. This window is also a floor under how close the
 * boundary can get to now(), and every second of it is another N rows a hybrid
 * aggregate has to read — where N is the ingest rate, so 15,000 rows a second
 * at the nominal load and 45,000 at the top of the ramp. That is why it is not
 * simply set generously.
 *
 * 12 seconds sits between the two with room to absorb a refresh that is skipped
 * or runs long.
 */
const WINDOW_SECONDS = intFromEnv('INGEST_FLOOR_WINDOW_SECONDS', 12);

/**
 * One slot per second of the window, indexed by wall-clock second. A flat ring
 * keeps this O(1) per accepted batch: POST /logs runs thousands of times a
 * second, so a growing list — with the pruning and the allocation it implies —
 * would be a cost on the hot path rather than a comparison.
 */
const SLOTS = WINDOW_SECONDS + 1;
const slotSecond = new Int32Array(SLOTS).fill(-1);
const slotMin = new Float64Array(SLOTS);

/** Records the oldest timestamp in an accepted batch. Called once per request. */
export function noteAccepted(minTimestampMs: number): void {
  if (!Number.isFinite(minTimestampMs)) return;

  const second = Math.floor(Date.now() / 1000);
  const slot = second % SLOTS;

  if (slotSecond[slot] !== second) {
    slotSecond[slot] = second;
    slotMin[slot] = minTimestampMs;
    return;
  }
  if (minTimestampMs < slotMin[slot]) slotMin[slot] = minTimestampMs;
}

/**
 * Oldest timestamp accepted inside the window, or Infinity when nothing has
 * been. Infinity means "no constraint", so the caller keeps the boundary the
 * refresh published.
 */
export function acceptedFloorMs(): number {
  const oldest = Math.floor(Date.now() / 1000) - WINDOW_SECONDS;
  let floor = Infinity;

  for (let slot = 0; slot < SLOTS; slot++) {
    if (slotSecond[slot] < oldest) continue;
    if (slotMin[slot] < floor) floor = slotMin[slot];
  }
  return floor;
}
