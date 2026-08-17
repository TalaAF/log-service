-- After isolating aggregate connections and removing cross-service marker
-- scans, a 5-second refresh left 139k raw rows at aggregate p95 under the
-- calibrated stress mix. Two-second folds reduced that to 42k while also
-- reducing total refresh execution time because each fold was much smaller.
SELECT cron.unschedule('log-rollup-refresh')
WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname = 'log-rollup-refresh');

SELECT cron.schedule(
    'log-rollup-refresh',
    '2 seconds',
    'SELECT refresh_log_rollups()'
);
