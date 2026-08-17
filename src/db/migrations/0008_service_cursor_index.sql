-- The calibrated read-after-write query and eventual-consistency walk both
-- constrain service before a timestamp range and order by the cursor tuple.
-- With only the timestamp-first index, each marker lookup reads every service
-- in its 10-second window and filters roughly eleven twelfths of those rows.
-- Keep the existing timestamp-first index for unfiltered pagination; this
-- service-first path is intentionally narrow and contains no wide INCLUDE
-- columns, limiting write amplification on the ingest-heavy workload.
CREATE INDEX IF NOT EXISTS idx_logs_service_timestamp_id
ON logs (service, "timestamp" DESC, id DESC);
