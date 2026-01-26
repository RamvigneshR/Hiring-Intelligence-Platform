import os
import json
from sqlalchemy import text
from datetime import datetime 
from dotenv import load_dotenv
from config.db import get_engine

load_dotenv()

storage_path = os.getenv("STORAGE_PATH")
run_date = os.getenv("RUN_DATE")

def update_run_date():
    #if set use RUN_DATE from .env or leave empty to use current utc date
    if run_date:
        return run_date
    return datetime.utcnow().strftime("%Y_%m_%d")

def main():
    if not storage_path:
        raise ValueError("storage_path not set")
    
    engine = get_engine()
    run_date = update_run_date()
    
    #collect files with run_date to fetch data to insert into raw.raw_jobs table
    files=[]
    for f in os.listdir(storage_path):
        if f.startswith(f"jobs_{run_date}_"):
            files.append(f)
    if not files:
            print(f"No files found for run_date={run_date}")
            return
    
    #insert data from the files of the run_date to raw.raw_jobs table
    with engine.begin() as conn:
        for file_name in files:
            file_path = os.path.join(storage_path,file_name)
            run_id = file_name.replace("jobs_","").split("_page_")[0]
            page = file_name.split("_page_")[1].split(".")[0]
            # print(page)
            
            with open(file_path,"r") as f:
                data = json.load(f)
            
            for job in data.get("data",[]):
                conn.execute(text("""
                                Insert into raw.raw_jobs
                                (job_id,payload,page,run_date,run_id)
                                values(:job_id,cast(:payload as jsonb),:page,:run_date,:run_id)"""),
                            {"job_id" : job.get("slug"),
                            "payload" : json.dumps(job),
                            "page" : page,
                            "run_date" : run_date,
                            "run_id" : run_id
                            }
                            )
        print(f"loaded {len(files)} files into raw.raw_jobs") 

if __name__=="__main__":
    main()      
            
                