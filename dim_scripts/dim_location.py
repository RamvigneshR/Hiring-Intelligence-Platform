from sqlalchemy import text
from config.db import get_engine
from datetime import datetime
import logging
from dotenv import load_dotenv
from log_script import log

def load_dim_company(run_id : str):
    logger = log(run_id)
    engine = get_engine()
    
    logger.info("Step 100: dim.dim_location started")
    sql = """Insert into dim.dim_location(city,state,country,is_remote,location)
            Select distinct city,state,country,is_remote,location from stage.stage_jobs
            where location is not null
            on conflict(location) do nothing"""

    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            logger.info(f"Step 400 : Successfully inserted {result.rowcount} rows into dim.dim_location")
    except Exception as e:
            logging.error(f"Step 200 : Cannot insert duplicates in dim_location")
            raise
        
if __name__ == "__main__":      
    run_id = "dim_location" + datetime.now().strftime("%Y%m%d")
    load_dim_company(run_id) 
    

