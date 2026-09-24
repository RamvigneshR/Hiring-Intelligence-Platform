from datetime import datetime
from airflow.decorators import dag, task
from pipeline.dimensions.load_dim_company import load_dim_company
from pipeline.dimensions.load_dim_date import load_dim_date
from pipeline.dimensions.load_dim_job_title import load_dim_job_title
from pipeline.dimensions.load_dim_location import load_dim_location


@dag(
    dag_id="arbeitnow_dim_jobs",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dimensions", "dimension_jobs", "load_dim_company","load_dim_date", "load_dim_job_title", "load_dim_location"]
)
def dimension_pipeline():

    @task
    def load_dim_company_task(ts_nodash:str = None):
        print("Loading dimension company records for run_id:", ts_nodash)
        load_dim_company(run_id=ts_nodash)

    @task
    def load_dim_date_task(ts_nodash:str = None):
        print("Loading dimension date records for run_id:", ts_nodash)
        load_dim_date(run_id=ts_nodash)

    @task
    def load_dim_job_title_task(ts_nodash:str = None):
        print("Loading dimension job title records for run_id:", ts_nodash)
        load_dim_job_title(run_id=ts_nodash)

    @task
    def load_dim_location_task(ts_nodash:str = None):   
        print("Loading dimension location records for run_id:", ts_nodash)
        load_dim_location(run_id=ts_nodash)

    dim_company = load_dim_company_task()
    dim_date = load_dim_date_task()
    dim_job_title = load_dim_job_title_task()   
    dim_location = load_dim_location_task()

dimension_pipeline()