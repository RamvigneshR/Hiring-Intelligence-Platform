from datetime import datetime
from airflow.decorators import task,dag
from pipeline.final.fact_load_jobs import load_fact_jobs

@dag(
    dag_id="arbeitnow_fact_job",
    schedule=None,
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=["fact","fact_jobs","load_fact_jobs","production"]
)

def fact_pipeline():
    @task
    def load_fact_task(run_id:str=None):
        run_id = f"fact_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"Loading fact database records with run_id: {run_id}")
        load_fact_jobs(run_id=run_id)

    fact_task=load_fact_task()

fact_pipeline()