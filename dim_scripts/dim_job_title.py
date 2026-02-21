from sqlalchemy import text
from config.db import get_engine
from datetime import datetime
import logging
from dotenv import load_dotenv
from log_script import log

def load_dim_job_title(run_id : str):
    logger = log(run_id)
    engine = get_engine()
    
    logger.info("Step 100: dim.dim_job_title started")
    sql = """Insert into dim.dim_job_title(job_title,seniority_level)
            Select distinct job_title,seniority_level from stage.stage_jobs
            where job_title is not null
            on conflict(job_title) do nothing"""

    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            logger.info(f"Step 400 : Successfully inserted {result.rowcount} rows into dim.dim_job_title")
    except Exception as e:
            logging.error(f"Step 200 : Cannot insert duplicates in dim_job_title")
            raise
        
if __name__ == "__main__":      
    run_id = "dim_job_title" + datetime.now().strftime("%Y%m%d")
    load_dim_job_title(run_id) 
    

