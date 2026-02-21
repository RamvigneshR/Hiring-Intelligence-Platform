from sqlalchemy import text
from config.db import get_engine
from datetime import datetime
import logging
from dotenv import load_dotenv
from log_script import log

def load_dim_date(run_id : str):
    logger = log(run_id)
    engine = get_engine()
    
    logger.info("Step 100: dim.dim_date started")
    sql = """Insert into dim.dim_date(date_key,full_date,year,quarter,month,month_name,week_of_year,day_of_month,day_of_week,day_name,is_weekend)
            Select 
            to_char(d,'YYYYMMDD') :: Integer as date_key,
            d as full_date,
            extract(year from d):: Integer as year,
            extract(quarter from d):: Integer as quarter,
            extract(month from d):: Integer as month,
            to_char(d,'Month') as month_name,
            extract(week from d):: Integer as week_of_year,            
            extract(day from d):: Integer as day_of_month,            
            extract(isodow from d):: Integer as day_of_week,  
            to_char(d,'Day') as day_name,           
            extract(isodow from d) in (6,7) as is_weekend
            from generate_series('2020-01-01' :: Date,'2100-12-31' :: Date, '1 day' :: Interval)d
            on conflict (full_date) do nothing"""

    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            logger.info(f"Step 400 : Successfully inserted {result.rowcount} rows into dim.dim_date")
    except Exception as e:
            logging.error(f"Step 200 : Cannot insert duplicates in dim_date")
            raise
        
if __name__ == "__main__":      
    run_id = "dim_date" + datetime.now().strftime("%Y%m%d")
    load_dim_date(run_id) 
    

