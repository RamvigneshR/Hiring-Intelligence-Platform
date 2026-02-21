from sqlalchemy import text
from config.db import get_engine
from datetime import datetime
import logging
from log_script import log

def load_dim_company(run_id : str):
    logger = log(run_id)
    engine = get_engine()
    
    logger.info("Step 100: dim.dim_company started")
    sql = """Insert into dim.dim_company(company_name)
            Select distinct company_name from stage.stage_jobs
            where company_name is not null
            on conflict(company_name) do nothing"""

    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            logger.info(f"Step 400 : Successfully inserted {result.rowcount} rows into dim.dim_company ")
            print(f"dim.dim_company loaded successfully with {result.rowcount} rows")
    except Exception as e:
            logging.error(f"Step 200 : Cannot insert duplicates in dim_company")
            raise
        
if __name__ == "__main__":
    run_id = "dim_company" + datetime.now().strftime("%Y%m%d")
    load_dim_company(run_id) 
    

