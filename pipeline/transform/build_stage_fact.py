from sqlalchemy import text
from datetime import datetime, timezone
from config.db import get_engine
from config.settings import validate
from utils.logger import get_logger


def build_stage_fact(run_id: str) -> int:
    # joins stage_jobs against dim tables and keeps only rows where every
    # required dim key was found. location is not required, remote jobs
    # never get a location match and that's expected, not a failure.
    validate()
    logger = get_logger("build_stage_fact", run_id)
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stage.stage_fact_jobs"))

        result = conn.execute(text("""
            INSERT INTO stage.stage_fact_jobs (
                job_id, dim_company_key, dim_location_key,
                dim_job_title_key, dim_posted_date_key,
                is_remote, experience_level, source_url, run_date
            )
            SELECT
                s.job_id,
                c.dim_company_key,
                l.dim_location_key,
                j.dim_job_title_key,
                d.dim_posted_date_key,
                s.is_remote,
                s.experience_level,
                s.source_url,
                s.run_date
            FROM stage.stage_jobs s
            JOIN dim.dim_company c ON s.company_name = c.company_name
            JOIN dim.dim_job_title j ON s.job_title = j.job_title
            JOIN dim.dim_date d ON s.posted_date_key = d.dim_posted_date_key
            LEFT JOIN dim.dim_location l ON s.location = l.location
        """))
        row_count = result.rowcount

    logger.info(f"stage.stage_fact_jobs: {row_count} inserted")
    return row_count


if __name__ == "__main__":
    run_id = f"stage_fact_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    build_stage_fact(run_id)