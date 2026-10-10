from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context


@dag(
    dag_id="http_traffic_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    params={"process_date": Param("2025-01-30", type="string", format="date")},
    tags=["http", "etl", "elt"],
)
def http_traffic_pipeline():
    @task(multiple_outputs=False)
    def prepare_day():
        from traffic import PROJECT_DIR, ensure_tables, parse_date

        context = get_current_context()
        process_date = context["params"]["process_date"]
        parse_date(process_date)
        source = PROJECT_DIR / "data" / "raw" / f"{process_date}.csv"
        if not source.is_file():
            raise FileNotFoundError(source)
        ensure_tables()
        return {
            "process_date": process_date,
            "source_path": str(source),
            "result_path": str(PROJECT_DIR / "work" / process_date / "daily_etl.csv"),
        }

    @task
    def calculate_etl(day):
        from traffic import transform_etl

        return transform_etl(day["source_path"], day["process_date"], day["result_path"])

    @task
    def load_etl_result(result_path, day):
        from traffic import load_etl

        return load_etl(result_path, day["process_date"])

    @task
    def load_raw_logs(day):
        from traffic import load_raw

        return load_raw(day["source_path"], day["process_date"])

    @task
    def calculate_elt(process_date):
        from traffic import transform_elt

        return transform_elt(process_date)

    @task(multiple_outputs=False)
    def compare_results(etl_date, elt_date):
        from traffic import compare_daily

        if etl_date != elt_date:
            raise ValueError("The branches processed different dates")
        result = compare_daily(etl_date)
        print(result)
        return result

    day = prepare_day()
    etl_date = load_etl_result(calculate_etl(day), day)
    elt_date = calculate_elt(load_raw_logs(day))
    compare_results(etl_date, elt_date)


http_traffic_pipeline()
