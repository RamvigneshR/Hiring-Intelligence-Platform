from sqlalchemy import text
from config.db import get_engine
from datetime import datetime
import logging
from dotenv import load_dotenv
from log_script import log


def load_stage_fact_jobs(run_id : str):
    logger = log(run_id)
    engine = get_engine()
    logger.info("Step 100: Loading staging fact table")
    
    truncate = "truncate table stage.stage_fact_jobs"
    
    sql = """
        Insert into stage.stage_fact_jobs (
            job_id,
            run_id,
            company_key,
            company_name,
            location_key,
            location,
            job_title_key,
            job_title,
            posted_date_key,
            is_remote,
            experience_level,
            source_url,
            run_date,
            is_valid,
            validation_error
        )
        SELECT 
            s.job_id,
            s.run_id,
            c.company_key,
            c.company_name,
            l.location_key,
            l.location,
            j.job_title_key,
            j.job_title,
            d.date_key AS posted_date_key,
            s.is_remote,
            s.experience_level,
            s.source_url,
            s.run_date,
            CASE 
                WHEN c.company_key IS NULL THEN FALSE
                WHEN l.location_key IS NULL THEN FALSE
                WHEN j.job_title_key IS NULL THEN FALSE
                WHEN d.date_key IS NULL THEN FALSE
                ELSE TRUE
            END AS is_valid,
            CASE 
                WHEN c.company_key IS NULL THEN 'Missing company_key for: ' || s.company_name
                WHEN l.location_key IS NULL THEN 'Missing location_key for: ' || s.location
                WHEN j.job_title_key IS NULL THEN 'Missing job_title_key for: ' || s.job_title
                WHEN d.date_key IS NULL THEN 'Missing date_key for: ' || s.posted_date_key::TEXT
                ELSE NULL
            END AS validation_error
        FROM stage.stage_jobs s
        LEFT JOIN dim.dim_company c ON s.company_name = c.company_name
        LEFT JOIN dim.dim_location l ON s.location = l.location
        LEFT JOIN dim.dim_job_title j ON s.job_title = j.job_title
        LEFT JOIN dim.dim_date d ON s.posted_date_key = d.date_key
    """
    try:
        with engine.begin() as conn:
            truncate_table = conn.execute(text(truncate))
            result = conn.execute(text(sql))
            count = result.rowcount
            print(f"Successfully inserted {result.rowcount} rows into stage.stage_fact_jobs")
            logger.info(f"Step 400 : Successfully inserted {result.rowcount} rows into stage.stage_fact_jobs")
    except Exception as e:
            logging.error(f"Step 200 : Cannot insert duplicates in stage.stage_fact_jobs")
            raise
        
if __name__ == "__main__":
    run_id = "stage_fact_jobs" + datetime.now().strftime("%Y%m%d")
    load_stage_fact_jobs(run_id) 
    