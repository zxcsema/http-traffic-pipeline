INSERT INTO traffic.traffic_daily_elt
    (event_date, path, requests, unique_ips, errors_4xx, errors_5xx, error_rate, avg_response_ms)
WITH
    deduplicated AS
    (
        SELECT DISTINCT request_id, timestamp, ip, method, path, status_code, response_ms, user_agent
        FROM traffic.raw_http_logs
        WHERE source_date = toDate(%(process_date)s)
    ),
    trimmed AS
    (
        SELECT
            trimBoth(request_id) AS request_id,
            trimBoth(timestamp) AS event_timestamp,
            trimBoth(ip) AS ip,
            trimBoth(method) AS method,
            trimBoth(path) AS original_path,
            trimBoth(status_code) AS status_text,
            trimBoth(response_ms) AS response_text,
            trimBoth(user_agent) AS user_agent
        FROM deduplicated
    ),
    typed AS
    (
        SELECT
            *,
            toDateTimeOrNull(event_timestamp, 'UTC') AS event_time,
            toUInt16OrNull(status_text) AS status,
            toFloat64OrNull(response_text) AS duration,
            splitByChar('?', original_path)[1] AS normalized_path
        FROM trimmed
    ),
    cleaned AS
    (
        SELECT toDate(event_time, 'UTC') AS event_date, normalized_path AS path, ip, status, duration
        FROM typed
        WHERE request_id != '' AND event_timestamp != '' AND ip != ''
            AND method != '' AND original_path != '' AND user_agent != ''
            AND event_time IS NOT NULL
            AND toString(event_time) = event_timestamp
            AND toDate(event_time, 'UTC') = toDate(%(process_date)s)
            AND match(status_text, '^[0-9]{3}$')
            AND status BETWEEN 100 AND 599
            AND match(response_text, '^-?[0-9]+([.][0-9]+)?$')
            AND duration IS NOT NULL AND isFinite(duration) AND duration >= 0
            AND startsWith(normalized_path, '/')
    )
SELECT
    event_date,
    path,
    count() AS requests,
    uniqExact(ip) AS unique_ips,
    countIf(status BETWEEN 400 AND 499) AS errors_4xx,
    countIf(status BETWEEN 500 AND 599) AS errors_5xx,
    coalesce(toFloat64(countIf(status BETWEEN 400 AND 599)) / nullIf(count(), 0), 0.) AS error_rate,
    avg(duration) AS avg_response_ms
FROM cleaned
GROUP BY event_date, path;
