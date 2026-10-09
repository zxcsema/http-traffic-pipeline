SELECT path, sum(requests) AS requests
FROM traffic.traffic_daily_elt
WHERE event_date BETWEEN '2025-01-30' AND '2025-02-02'
GROUP BY path
ORDER BY requests DESC, path
LIMIT 10;

SELECT
    path,
    sum(requests) AS requests,
    sum(errors_4xx + errors_5xx) AS errors,
    coalesce(toFloat64(errors) / nullIf(requests, 0), 0.) AS error_rate
FROM traffic.traffic_daily_elt
WHERE event_date BETWEEN '2025-01-30' AND '2025-02-02'
GROUP BY path
HAVING requests >= 100
ORDER BY error_rate DESC, path
LIMIT 10;

SELECT
    event_date,
    sum(requests) AS total_requests,
    sum(errors_4xx) AS errors_4xx,
    sum(errors_5xx) AS errors_5xx,
    coalesce(toFloat64(errors_4xx + errors_5xx) / nullIf(total_requests, 0), 0.) AS error_rate,
    sum(avg_response_ms * requests) / nullIf(total_requests, 0) AS weighted_avg_response_ms
FROM traffic.traffic_daily_elt
WHERE event_date BETWEEN '2025-01-30' AND '2025-02-02'
GROUP BY event_date
ORDER BY event_date;

SELECT
    table,
    partition,
    count() AS active_parts,
    sum(rows) AS rows,
    sum(marks) AS marks,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE database = 'traffic'
    AND table IN ('raw_http_logs', 'traffic_daily_etl', 'traffic_daily_elt')
    AND active
GROUP BY table, partition
ORDER BY table, partition;
