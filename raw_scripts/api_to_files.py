import os
import json
import requests
from datetime import datetime,timezone
from dotenv import load_dotenv

#load environment variables from .env
load_dotenv()

website_url = os.getenv("WEBSITE_URL")
pages_to_run = int(os.getenv("PAGES_TO_RUN") or 1)
storage_path = os.getenv("STORAGE_PATH")

def main():
    if not website_url or not storage_path:
        raise ValueError("Website url or storage path not set")
    os.makedirs(storage_path,exist_ok=True)
    
    run_date = datetime.now(timezone.utc).strftime("%Y_%m_%d")
    run_time = datetime.now(timezone.utc).strftime("%H%M%S")
    run_id = f"{run_date}_{run_time}" 
    
    #fetch data from each page
    for page in range(1,pages_to_run+1):
        url = f"{website_url}?page={page}"
        response = requests.get(url,timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        #save json file
        file_name = f"jobs_{run_id}_page_{page}"
        file_path = os.path.join(storage_path,file_name)
        
        with open(file_path,"w") as f:
            json.dump(data,f,indent=2)
            
        print(f"File created {file_name}")
        
if __name__ == "__main__":
    main()