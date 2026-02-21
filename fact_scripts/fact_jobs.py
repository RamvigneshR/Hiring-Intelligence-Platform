from sqlalchemy import text
from config.db import get_engine
from datetime import datetime
import logging
from dotenv import load_dotenv
from log_script import log


def load_fact_jobs(run_id : str):
    logger = log(run_id)
    engine = get_engine()
    logger.info("Step 100: Loading fact table")
        
    sql = """
        Insert into fact.fact_jobs (
            job_id,
            company_key,
            location_key,
            job_title_key,
            posted_date_key,
            is_remote,
            experience_level,
            source_url,
            run_id,
            run_date
        )
        SELECT 
            job_id,
            company_key,
            location_key,
            job_title_key,
            posted_date_key,
            is_remote,
            experience_level,
            source_url,
            run_id,
            run_date
            from stage.stage_fact_jobs
            where is_valid = TRUE
            on conflict(job_id,run_id) do nothing
    """
    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            count = result.rowcount
            print(f"Successfully inserted {result.rowcount} rows into fact.fact_jobs")
            logger.info(f"Step 400 : Successfully inserted {result.rowcount} rows into fact.fact_jobs")
    except Exception as e:
            logging.error(f"Step 200 : Cannot insert duplicates in fact.fact_jobs")
            raise
        
if __name__ == "__main__":
    run_id = "fact_jobs" + datetime.now().strftime("%Y%m%d")
    load_fact_jobs(run_id) 
    