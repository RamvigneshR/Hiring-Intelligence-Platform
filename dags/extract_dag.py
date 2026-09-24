from datetime import datetime, timezone
from airflow.decorators import dag, task
from pipeline.extract.fetch_arbeitnow import run_fetch
from pipeline.extract.load_raw_jobs import load_raw_jobs


@dag(
    dag_id="arbeitnow_extract_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["extract", "jobs", "production"],
)
def extract_pipeline():

    @task
    def fetch_task(ds_nodash: str = None):
        run_date = datetime.now(timezone.utc).strftime("%Y%m%d")
        run_fetch(run_date=run_date)
        return run_date

    @task
    def load_raw_task(run_date: str):
        print(f"Loading raw database records for run_date: {run_date}")
        load_raw_jobs(run_date=run_date)

    date_var = fetch_task()
    load_raw_task(date_var)


extract_pipeline()