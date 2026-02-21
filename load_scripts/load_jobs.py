from config.db import get_engine
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import text
import os

load_dotenv()

run_start_date = os.getenv("RUN_START_DATE")
run_end_date = os.getenv("RUN_END_DATE")

def define_start_end_date():
    if run_end_date and run_start_date:
        return run_start_date,run_end_date
    today = datetime.utcnow().strftime("%Y_%m_%d")
    return str(today),str(today)

def main():
    engine = get_engine()
    start_date,end_date = define_start_end_date()
    
    with engine.begin() as conn:
        conn.execute(text("Truncate table load.load_jobs restart identity"))
        conn.execute(text("""
                          Insert into load.load_jobs(
                            run_id,
                            job_id,
                            role,
                            description,
                            company_name,
                            location,
                            is_remote,
                            source_url,
                            posted_time,
                            run_date)
                            Select 
                            run_id,
                            payload->>'slug' as job_id,
                            payload->>'title' as Role,
                            payload->>'description' as description,
                            payload->>'company_name' as company_name,
                            payload->>'location' as location,
                            (payload->>'remote'):: boolean as is_remote,
                            payload->>'url' as source_url,
                            payload->>'created_at' as posted_time,
                            run_date
                            from raw.raw_jobs
                            where run_date between :start_date and :end_date;
                            """),
                    {"start_date" : start_date,"end_date" : end_date})
        result = conn.execute(text("""select count(*) from load.load_jobs 
                            where run_date between :start_date and :end_date;"""),
                              {"start_date" : start_date,"end_date" : end_date})
        total_rows = result.scalar()
        print(f"{total_rows} rows loaded into load.load_jobs")
         
if __name__ == "__main__":
    main()
        
   
    