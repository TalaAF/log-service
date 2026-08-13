-- Seeds ~1M rows spread evenly across the last 30 days, matching the stated
-- baseline of "~1,000,000 stored log records representing ~1 month of data".
INSERT INTO logs ("timestamp", level, service, message, attributes)
SELECT
    now() - (random() * interval '30 days'),
    (ARRAY['debug','info','warn','error'])[1 + (n % 4)],
    (ARRAY['checkout','auth','payments','search','inventory','shipping','notify','gateway'])[1 + (n % 8)],
    (ARRAY['payment declined','user login succeeded','cache miss for key',
           'upstream timeout after 3000ms','order committed to ledger',
           'rate limit applied to tenant','index rebuild finished',
           'connection reset by peer'])[1 + (n % 8)] || ' #' || n,
    jsonb_build_object(
        'user_id', (n % 10000)::text,
        'region',  (ARRAY['eu-west','us-east','ap-south','sa-east'])[1 + (n % 4)],
        'retries', n % 5,
        'cached',  (n % 2 = 0)
    )
FROM generate_series(1, 1000000) AS n;
