from sqlalchemy import text
from datetime import datetime, timezone
from config.db import get_engine
from config.settings import validate
from utils.logger import get_logger


def load_dim_job_title(run_id: str) -> int:
    validate()
    logger = get_logger("load_dim_job_title", run_id)
    engine = get_engine()

    sql = """
        INSERT INTO dim.dim_job_title (job_title, seniority_level)
        SELECT DISTINCT job_title, seniority_level
        FROM stage.stage_jobs
        WHERE job_title IS NOT NULL
        ON CONFLICT (job_title) DO NOTHING
    """

    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.rowcount

    logger.info(f"Inserted {rows} new rows into dim.dim_job_title")
    return rows


if __name__ == "__main__":
    run_id = f"dim_job_title_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    load_dim_job_title(run_id)