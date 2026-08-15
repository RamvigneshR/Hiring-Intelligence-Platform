from sqlalchemy import text
from datetime import datetime, timezone
from config.db import get_engine
from config.settings import validate
from utils.logger import get_logger


def load_fact_jobs(run_id: str) -> int:
    validate()
    logger = get_logger("load_fact_jobs", run_id)
    engine = get_engine()

    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO fact.fact_jobs (
                job_id, dim_company_key, dim_location_key,
                dim_job_title_key, dim_posted_date_key,
                is_remote, experience_level, source_url, run_date
            )
            SELECT
                job_id, dim_company_key, dim_location_key,
                dim_job_title_key, dim_posted_date_key,
                is_remote, experience_level, source_url, run_date
            FROM stage.stage_fact_jobs
            ON CONFLICT (job_id, run_date) DO NOTHING
        """))
        row_count = result.rowcount

    logger.info(f"fact.fact_jobs: {row_count} rows inserted")
    return row_count


if __name__ == "__main__":
    run_id = f"fact_jobs_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    load_fact_jobs(run_id)