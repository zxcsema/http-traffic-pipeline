import csv
import math
import os
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path


FIELDS = [
    "request_id", "timestamp", "ip", "method", "path", "status_code", "response_ms", "user_agent"
]
METRICS = [
    "event_date", "path", "requests", "unique_ips", "errors_4xx", "errors_5xx", "error_rate", "avg_response_ms"
]
PROJECT_DIR = Path(__file__).resolve().parent
SQL_DIR = PROJECT_DIR / "sql"


def parse_date(value):
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("Use a date in YYYY-MM-DD format")
    day = date.fromisoformat(value)
    if not date(1970, 1, 1) <= day <= date(2105, 12, 31):
        raise ValueError("The supported date range is 1970-01-01 to 2105-12-31")
    return day


def read_logs(path):
    with Path(path).open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != FIELDS:
            raise ValueError(f"Unexpected CSV header in {path}")
        rows = list(reader)
    if any(None in row or any(value is None for value in row.values()) for row in rows):
        raise ValueError(f"Wrong number of CSV fields in {path}")
    return rows


def clean_logs(rows, process_date):
    day = parse_date(process_date)
    seen = set()
    cleaned = []
    duplicates = 0
    invalid = 0
    for raw in rows:
        original = tuple(raw[field] for field in FIELDS)
        if original in seen:
            duplicates += 1
            continue
        seen.add(original)
        row = {field: raw[field].strip(" ") for field in FIELDS}
        if not all(row.values()):
            invalid += 1
            continue
        timestamp = row["timestamp"]
        status = row["status_code"]
        duration = row["response_ms"]
        if (
            not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", timestamp)
            or not re.fullmatch(r"[0-9]{3}", status)
            or not re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", duration)
        ):
            invalid += 1
            continue
        try:
            event_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            status_code = int(status)
            response_ms = float(duration)
        except ValueError:
            invalid += 1
            continue
        path = row["path"].split("?", 1)[0]
        if (
            event_time.date() != day
            or not 100 <= status_code <= 599
            or not math.isfinite(response_ms)
            or response_ms < 0
            or not path.startswith("/")
        ):
            invalid += 1
            continue
        cleaned.append({
            "event_date": day, "path": path, "ip": row["ip"],
            "status_code": status_code, "response_ms": response_ms,
        })
    return cleaned, {
        "source_rows": len(rows), "duplicates": duplicates, "invalid_rows": invalid, "valid_rows": len(cleaned)
    }


def aggregate_logs(cleaned):
    groups = defaultdict(list)
    for row in cleaned:
        groups[row["event_date"], row["path"]].append(row)
    result = []
    for (day, path), rows in sorted(groups.items()):
        requests = len(rows)
        errors_4xx = sum(400 <= row["status_code"] <= 499 for row in rows)
        errors_5xx = sum(500 <= row["status_code"] <= 599 for row in rows)
        result.append([
            day, path, requests, len({row["ip"] for row in rows}), errors_4xx, errors_5xx,
            (errors_4xx + errors_5xx) / requests,
            math.fsum(row["response_ms"] for row in rows) / requests,
        ])
    return result


def write_csv(path, fields, rows):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(fields)
        writer.writerows(rows)
    temporary.replace(destination)


def transform_etl(source_path, process_date, output_path):
    cleaned, statistics = clean_logs(read_logs(source_path), process_date)
    result = aggregate_logs(cleaned)
    write_csv(output_path, METRICS, result)
    print({"process_date": process_date, **statistics, "groups": len(result)})
    return str(output_path)


def get_client():
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "traffic"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="traffic",
        send_receive_timeout=120,
    )


def ensure_tables():
    with get_client() as client:
        for statement in (SQL_DIR / "schema.sql").read_text(encoding="utf-8").split(";"):
            if statement.strip():
                client.command(statement)


def delete_day(client, table, process_date):
    column = "source_date" if table == "raw_http_logs" else "event_date"
    if table not in ("raw_http_logs", "traffic_daily_etl", "traffic_daily_elt"):
        raise ValueError("Unexpected table")
    client.command(
        f"ALTER TABLE traffic.{table} DELETE WHERE {column} = toDate(%(process_date)s)",
        parameters={"process_date": parse_date(process_date)},
        settings={"mutations_sync": 1},
    )


def load_etl(result_path, process_date):
    day = parse_date(process_date)
    with Path(result_path).open(newline="", encoding="utf-8") as source:
        rows = [
            [parse_date(row["event_date"]), row["path"],
             *[int(row[field]) for field in METRICS[2:6]],
             float(row["error_rate"]), float(row["avg_response_ms"])]
            for row in csv.DictReader(source)
        ]
    if any(row[0] != day for row in rows):
        raise ValueError("The aggregate contains a different date")
    with get_client() as client:
        delete_day(client, "traffic_daily_etl", process_date)
        if rows:
            client.insert("traffic_daily_etl", rows, column_names=METRICS)
    return process_date


def load_raw(source_path, process_date):
    day = parse_date(process_date)
    rows = [[day, *[row[field] for field in FIELDS]] for row in read_logs(source_path)]
    with get_client() as client:
        delete_day(client, "raw_http_logs", process_date)
        if rows:
            client.insert("raw_http_logs", rows, column_names=["source_date", *FIELDS])
    return process_date


def transform_elt(process_date):
    with get_client() as client:
        delete_day(client, "traffic_daily_elt", process_date)
        client.command(
            (SQL_DIR / "transform_elt.sql").read_text(encoding="utf-8"),
            parameters={"process_date": parse_date(process_date)},
        )
    return process_date


def read_daily(client, table, process_date):
    if table not in ("traffic_daily_etl", "traffic_daily_elt"):
        raise ValueError("Unexpected table")
    return client.query(
        f"SELECT {', '.join(METRICS)} FROM traffic.{table} "
        "WHERE event_date = {process_date:Date} ORDER BY event_date, path",
        parameters={"process_date": parse_date(process_date)},
    ).result_set


def compare_daily(process_date):
    with get_client() as client:
        etl = read_daily(client, "traffic_daily_etl", process_date)
        elt = read_daily(client, "traffic_daily_elt", process_date)
    if len(etl) != len(elt):
        raise ValueError("ETL and ELT have different group counts")
    for left, right in zip(etl, elt):
        if tuple(left[:6]) != tuple(right[:6]):
            raise ValueError(f"Different keys or counters: {left} != {right}")
        for index in (6, 7):
            if not math.isclose(left[index], right[index], rel_tol=1e-9, abs_tol=1e-9):
                raise ValueError(f"Different {METRICS[index]} for {left[:2]}")
    return {"process_date": process_date, "groups": len(etl), "result": "ETL = ELT"}
