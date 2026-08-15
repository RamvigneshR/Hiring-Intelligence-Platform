from sqlalchemy import text
from datetime import datetime, timezone
from config.db import get_engine
from config.settings import validate
from utils.logger import get_logger
import argparse

def load_stage_jobs(start_date: str, end_date: str):
    validate()
    logger = get_logger("load_stage_jobs", f"{start_date}_{end_date}")
    engine = get_engine()

    with engine.begin() as conn:
        #conn.execute(text("TRUNCATE TABLE load.load_stage_jobs"))
        
        conn.execute(text("TRUNCATE TABLE load.load_stage_jobs RESTART IDENTITY"))

        conn.execute(
            text("""
                INSERT INTO load.load_stage_jobs (job_id, role, description, company_name, location,
                                             is_remote, source_url, posted_time, run_date)
                SELECT payload->>'slug', payload->>'title', payload->>'description',
                       payload->>'company_name', payload->>'location',
                       (payload->>'remote')::boolean, payload->>'url', payload->>'created_at', run_date
                FROM raw.raw_jobs
                WHERE run_date BETWEEN :start_date AND :end_date
            """),
            {"start_date": start_date, "end_date": end_date},
        )

        total_rows = conn.execute(text("SELECT count(*) FROM load.load_stage_jobs")).scalar()

    logger.info(f"Reloaded load.load_stage_jobs: {total_rows} rows for range {start_date} to {end_date}")
    return total_rows


if __name__ == "__main__":
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default=today, help="YYYYMMDD, defaults to today")
    parser.add_argument("--end-date", default=today, help="YYYYMMDD, defaults to today")
    args = parser.parse_args()
    load_stage_jobs(args.start_date, args.end_date)