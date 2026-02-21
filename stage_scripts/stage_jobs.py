from sqlalchemy import text
from config.db import get_engine
import logging
from datetime import datetime,date
import re
import os
from dotenv import load_dotenv

load_dotenv()
log_dir = "LOGLOG_DIR"
log_file_name=""

def log(layer,run_id = '12345'):
    date_str = datetime.now().strftime("%Y%m%d")
    
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file_name = os.path.join(log_dir, f"{layer}_{run_id}_{date_str}.log")
    
    logging.basicConfig(    
        filename = log_file_name,
        level = logging.INFO,
        format = "%(asctime)s %(levelname)s %(message)s",
    ) 
    return logging

def remove_html_tags(text):
    if text is not None:
        clean_desc = re.sub(r"<[^>]+>","",text)
        return clean_desc

def experince_level(text):
    text = text.lower()
    if "senior" in text or "lead" in text:
        return "senior"
    if "junior" in text or "graduate" in text or "entry" in text:
        return "fresher"
    return "mid"

def seniority(job_title):
    job_title_lower = job_title.lower()
    if "senior" in job_title_lower or "sr." in job_title_lower or "lead" in job_title_lower:
        return "senior"
    elif "junior" in job_title_lower or "jr." in job_title_lower:
        return "junior"
    else:
        return "mid"

def main():
    engine = get_engine()
    
    run_id = datetime.now().strftime("%Y_%m_%d_%H%M%S")
    logger = log("stage", run_id)
    
    try:
        with engine.begin() as conn:
            logger.info("Step 100: Stage_jobs task started")
            conn.execute(text("TRUNCATE TABLE stage.stage_jobs"))
            
            rows = conn.execute(text("""
                SELECT
                    job_id,
                    company_name,
                    role,
                    location,
                    is_remote,
                    posted_time,
                    description,
                    source_url,
                    run_date,
                    run_id
                FROM load.load_jobs
            """)).fetchall()
            
            for r in rows:
                clean_desc = remove_html_tags(r.description)
                experience = experince_level(clean_desc) 
                whole_location = r.location.lower().strip().split(',') if r.location else ['', '', '']
                city = whole_location[0].strip() if len(whole_location) > 0 else None
                state = whole_location[1].strip() if len(whole_location) > 1 else None
                country = whole_location[2].strip() if len(whole_location) > 2 else None
                seniority_level = seniority(r.role)
                
                posted_date = datetime.fromtimestamp(int(r.posted_time)).date()
                posted_date_key = int(posted_date.strftime("%Y%m%d"))
                
                conn.execute(text("""
                    Insert into stage.stage_jobs (
                        job_id,
                        company_name,
                        job_title,
                        seniority_level,
                        location,
                        city,
                        state,
                        country,
                        is_remote,
                        posted_time,
                        posted_date_key,
                        job_description,
                        experience_level,
                        source_url,
                        run_date,
                        run_id
                    )
                    VALUES (
                        :job_id,
                        :company_name,
                        :job_title,
                        :seniority_level,
                        :location,
                        :city,
                        :state,
                        :country,
                        :is_remote,
                        :posted_time,
                        :posted_date_key,
                        :job_description,
                        :experience_level,
                        :source_url,
                        :run_date,
                        :run_id
                    )
                """),
                {
                    "job_id": r.job_id,
                    "company_name": r.company_name.strip(),
                    "company_name_clean": r.company_name.strip(),
                    "job_title": r.role,
                    "seniority_level": seniority_level,
                    "location": r.location,
                    "city": city,
                    "state": state,
                    "country": country,
                    "is_remote": r.is_remote,
                    "posted_time": posted_date,
                    "posted_date_key":posted_date_key,
                    "job_description": clean_desc,
                    "experience_level": experience,
                    "source_url": r.source_url,
                    "run_date": r.run_date,
                    "run_id": r.run_id
                })
            
            duplicate_check = conn.execute(text("""
                select job_id, posted_time, run_id, count(*)
                from stage.stage_jobs
                group by job_id, posted_time, run_id
                having count(*) > 1
            """)).fetchall()
            
            if duplicate_check:
                raise Exception(f"Step 200: Duplicate records found in stage.stage_jobs: {duplicate_check}")
            
            countrows = conn.execute(text("select count(*) from stage.stage_jobs")).scalar()
            logger.info(f"Step 400: Successfully loaded {countrows} into stage.stage_jobs")
            print(f"stage.stage_jobs loaded successfully with {countrows} rows")
                
    except Exception as e:
        logger.error(str(e))
        print(f"Stage.stage_jobs failed - check logs/{log_file_name}")
        raise 
        
if __name__=="__main__":
    main()