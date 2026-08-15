from sqlalchemy import text
from datetime import datetime, timezone
from config.db import get_engine
from config.settings import validate
from utils.logger import get_logger


def build_filter_stage_fact(run_id: str) -> int:
    # captures rows from stage_jobs that failed to match company, job_title,
    # or posted_date in the dim tables. kept separate from stage_fact_jobs
    # so rejects can be investigated on their own without touching clean data.
    validate()
    logger = get_logger("build_filter_stage_fact", run_id)
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stage.filter_stage_fact_jobs"))

        result = conn.execute(text("""
            INSERT INTO stage.filter_stage_fact_jobs (
                job_id, company_name, location, job_title,
                posted_date_key, run_date, rejection_reason
            )
            SELECT
                s.job_id, s.company_name, s.location, s.job_title,
                s.posted_date_key, s.run_date,
                CASE
                    WHEN NOT EXISTS (SELECT 1 FROM dim.dim_company c WHERE c.company_name = s.company_name)
                        THEN 'Missing dim_company_key for company_name: ' || COALESCE(s.company_name, 'NULL')
                    WHEN NOT EXISTS (SELECT 1 FROM dim.dim_job_title j WHERE j.job_title = s.job_title)
                        THEN 'Missing dim_job_title_key for job_title: ' || COALESCE(s.job_title, 'NULL')
                    WHEN NOT EXISTS (SELECT 1 FROM dim.dim_date d WHERE d.dim_posted_date_key = s.posted_date_key)
                        THEN 'Missing dim_posted_date_key for posted_date_key: ' || s.posted_date_key::text
                    ELSE 'Unknown rejection reason'
                END
            FROM stage.stage_jobs s
            WHERE NOT EXISTS (SELECT 1 FROM dim.dim_company c WHERE c.company_name = s.company_name)
               OR NOT EXISTS (SELECT 1 FROM dim.dim_job_title j WHERE j.job_title = s.job_title)
               OR NOT EXISTS (SELECT 1 FROM dim.dim_date d WHERE d.dim_posted_date_key = s.posted_date_key)
        """))
        row_count = result.rowcount

    if row_count > 0:
        logger.error(f"{row_count} rows rejected — see stage.filter_stage_fact_jobs for details")
    else:
        logger.info("No rejected rows this run")

    return row_count


if __name__ == "__main__":
    run_id = f"filter_stage_fact_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    build_filter_stage_fact(run_id)