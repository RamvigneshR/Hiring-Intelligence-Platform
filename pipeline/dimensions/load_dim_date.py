from sqlalchemy import text
from datetime import datetime, timezone
from config.db import get_engine
from config.settings import validate
from utils.logger import get_logger


def load_dim_date(run_id: str) -> int:
    validate()
    logger = get_logger("load_dim_date", run_id)
    engine = get_engine()

    sql = """
        INSERT INTO dim.dim_date (
            dim_posted_date_key, full_date, year, quarter, month, month_name,
            week_of_year, day_of_month, day_of_week, day_name, is_weekend
        )
        SELECT
            to_char(d, 'YYYYMMDD')::integer,
            d,
            extract(year from d)::integer,
            extract(quarter from d)::integer,
            extract(month from d)::integer,
            to_char(d, 'Month'),
            extract(week from d)::integer,
            extract(day from d)::integer,
            extract(isodow from d)::integer,
            to_char(d, 'Day'),
            extract(isodow from d) in (6, 7)
        FROM generate_series('2020-01-01'::date, '2100-12-31'::date, '1 day'::interval) d
        ON CONFLICT (full_date) DO NOTHING
    """

    with engine.begin() as conn:
        result = conn.execute(text(sql))
        rows = result.rowcount

    logger.info(f"Inserted {rows} new rows into dim.dim_date")
    return rows


if __name__ == "__main__":
    run_id = f"dim_date_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    load_dim_date(run_id)