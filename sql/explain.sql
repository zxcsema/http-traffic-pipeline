EXPLAIN indexes = 1
SELECT sum(requests)
FROM traffic.traffic_daily_elt
WHERE event_date = {process_date:Date} AND path = '/catalog'
SETTINGS use_query_condition_cache = 0;
