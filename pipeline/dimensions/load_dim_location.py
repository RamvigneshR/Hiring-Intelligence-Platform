from sqlalchemy import text
from datetime import datetime, timezone
from config.db import get_engine
from config.settings import validate
from utils.logger import get_logger


def load_dim_location(run_id: str) -> int:
    validate()
    logger = get_logger("load_dim_location", run_id)
    engine = get_engine()
    
    sql = """
        INSERT INTO dim.dim_location (city, state, country, is_remote, location)
        SELECT DISTINCT city, state, country, is_remote, location
        FROM stage.stage_jobs
        WHERE location IS NOT NULL
        ON CONFLICT (location) DO NOTHING
    """

    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.rowcount

    logger.info(f"Inserted {rows} new rows into dim.dim_location")
    return rows


if __name__ == "__main__":
    run_id = f"dim_location_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    load_dim_location(run_id)