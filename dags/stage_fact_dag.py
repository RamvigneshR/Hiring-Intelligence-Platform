from datetime import datetime
from airflow.decorators import dag, task
from pipeline.transform.build_stage_fact import build_stage_fact
from pipeline.transform.build_filter_stage_fact import build_filter_stage_fact

@dag(
    dag_id = "arbeitnow_stage_fact",
    schedule=None,
    start_date=datetime(2026,1,1),
    catchup=False,
    tags=["stage_fact","filter_fact","deduplicate_stage_fact","prod"]
)

def stage_fact_pipeline():
    
    @task
    def build_stage_fact_task(ts_nodash:str=None):
        print(f"loading stage_fact with run_id:",ts_nodash)
        build_stage_fact(run_id=ts_nodash)

    @task
    def build_filter_stage_fact_task(ts_nodash:str=None):
        print(f"loading filter_stage_fact with run_id:",ts_nodash)
        build_filter_stage_fact(run_id=ts_nodash)

    stage_fact = build_stage_fact_task()
    filter_stage_fact = build_filter_stage_fact_task()

    stage_fact>>filter_stage_fact

stage_fact_pipeline()
