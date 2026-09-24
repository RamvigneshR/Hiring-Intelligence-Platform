from datetime import datetime, timezone
from airflow.decorators import dag, task
from pipeline.transform.load_stage_jobs import load_stage_jobs
from pipeline.transform.clean_stage_jobs import clean_stage_jobs

@dag(
    dag_id="arbeitnow_stage_jobs",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stage", "stage_jobs", "load_stage_jobs","clean_stage_jobs", "production"]
)
def stage_pipeline():

    @task
    def load_stage_task(start_date: str = None, end_date: str = None):
        start_date = datetime.now(timezone.utc).strftime("%Y%m%d")
        end_date = datetime.now(timezone.utc).strftime("%Y%m%d")
        print(f"Loading stage database records for range: {start_date} to {end_date}")        
        load_stage_jobs(start_date=start_date, end_date=end_date)

    @task
    def clean_stage_task(start_date: str = None, end_date: str = None):
        start_date = datetime.now(timezone.utc).strftime("%Y%m%d")
        end_date = datetime.now(timezone.utc).strftime("%Y%m%d")
        print(f"Cleaning stage database records for range: {start_date} to {end_date}")
        clean_stage_jobs(start_date=start_date, end_date=end_date)

    # Wire dependencies via the run_date token
    load_stage_task() >> clean_stage_task()


stage_pipeline()