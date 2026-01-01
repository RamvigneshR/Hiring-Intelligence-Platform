import os
import json
import requests
from datetime import datetime,timezone
from sqlalchemy import text
from config.db import get_engine
from dotenv import load_dotenv

load_dotenv()

websiteUrl = os.getenv("WEBSITE_URL")
pageToFetch = int(os.getenv("PAGES_TO_RUN")or 1)
storagePath = os.getenv("STORAGE_PATH")


def fetch_api_page(page:int):
    
    wurl = f"{websiteUrl}?page={page}" 
    response = requests.get(wurl,timeout=30)
    response.raise_for_status()
    return response.json()

def save_to_storage_path(data:dict,run_date:str,page:int):
    os.makedirs(storagePath,exist_ok = True)
    fileName = f"job_{run_date}_page_{page}.json"
    filePath = os.path.join(storagePath,fileName)
    
    with open(filePath,"w") as f:
        json.dump(data,f,indent=2)
    
    
def insert_into_table(engine,data,page):
    
    rowInserted = 0
    
    with engine.begin() as conn:
        for job in data.get("data",[]):
            conn.execute(
                text(
                     """insert into raw.fetch_from_api_abnow
                        (job_id,payload,page,dss_load_time)
                        values(:job_id,cast(:payload as jsonb),:page,now())"""
                ),
                {
                    "job_id" : job.get("slug"),
                    "payload" : json.dumps(job),
                    "page" : page  
                }
            )            
            rowInserted+=1
    return rowInserted

def main():
    
    if not websiteUrl:
        raise ValueError("Set variable in .env")
    
    engine = get_engine()
    
    run_date = datetime.now(timezone.utc).strftime("%Y_%m_%d")
    
    totalRows = 0
    
    for page in range(1,pageToFetch+1):
        api_data = fetch_api_page(page)
        save_to_storage_path(api_data,run_date,page)
        insertIntoTable = insert_into_table(engine,api_data,page)
        
        totalRows += insertIntoTable
        
        print(f"Inserted {totalRows} into raw.fetch_from_api_abnow table")
        
if __name__ == '__main__':
    main()