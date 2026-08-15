from sqlalchemy import text
from datetime import datetime, timezone
from config.db import get_engine
from config.settings import validate
from utils.logger import get_logger


def load_dim_company(run_id: str) -> int:
    validate()
    logger = get_logger("load_dim_company", run_id)
    engine = get_engine()

    sql = """
        INSERT INTO dim.dim_company (company_name)
        SELECT DISTINCT company_name FROM stage.stage_jobs
        WHERE company_name IS NOT NULL
        ON CONFLICT (company_name) DO NOTHING
    """

    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.rowcount

    logger.info(f"Inserted {rows} new rows into dim.dim_company")
    return rows


if __name__ == "__main__":
    run_id = f"dim_company_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    load_dim_company(run_id)