import json
import os
from sqlalchemy import text
from datetime import datetime, timezone
from config.db import get_engine
from config.settings import STORAGE_PATH, validate
from utils.logger import get_logger


def get_unloaded_files(conn, run_date: str):
    all_files = [
        os.path.join(STORAGE_PATH, f)
        for f in os.listdir(STORAGE_PATH)
        if f.startswith(f"jobs_{run_date}")
    ]
    if not all_files:
        return []

    loaded = conn.execute(
        text("SELECT file_path FROM raw.etl_file_load WHERE layer='raw' AND status='success' AND run_date=:run_date"),
        {"run_date": run_date},
    ).fetchall()
    loaded_paths = {row[0] for row in loaded}

    return [f for f in all_files if f not in loaded_paths]


def load_raw_jobs(run_date: str) -> int:
    validate()
    logger = get_logger("load_raw_jobs", run_date)
    engine = get_engine()
    total_rows = 0

    with engine.begin() as conn:
        files = get_unloaded_files(conn, run_date)
        if not files:
            logger.info(f"No new files to load for run_date={run_date}")
            return 0

        for file_path in files:
            page = file_path.split("_page_")[1].split(".")[0]
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)

                rows_this_file = 0
                for job in data.get("data", []):
                    conn.execute(
                        text("""
                            INSERT INTO raw.raw_jobs (job_id, payload, page, run_date)
                            VALUES (:job_id, CAST(:payload AS jsonb), :page, :run_date)
                            ON CONFLICT (job_id, run_date) DO NOTHING
                        """),
                        {"job_id": job.get("slug"), "payload": json.dumps(job),
                         "page": page, "run_date": run_date},
                    )
                    rows_this_file += 1

                conn.execute(
                    text("""
                        INSERT INTO raw.etl_file_load (file_path, run_date, layer, status, row_count)
                        VALUES (:file_path, :run_date, 'raw', 'success', :row_count)
                        ON CONFLICT (file_path) DO UPDATE
                        SET status='success', row_count=:row_count, loaded_at=now()
                    """),
                    {"file_path": file_path, "run_date": run_date, "row_count": rows_this_file},
                )
                total_rows += rows_this_file
                logger.info(f"Loaded {file_path}: {rows_this_file} rows")

            except Exception as e:
                conn.execute(
                    text("""
                        INSERT INTO raw.etl_file_load (file_path, run_date, layer, status, row_count)
                        VALUES (:file_path, :run_date, 'raw', 'failed', 0)
                        ON CONFLICT (file_path) DO UPDATE SET status='failed', loaded_at=now()
                    """),
                    {"file_path": file_path, "run_date": run_date},
                )
                logger.error(f"Failed loading {file_path}: {e}")
                raise

    logger.info(f"Total loaded for run_date={run_date}: {total_rows} rows")
    return total_rows


if __name__ == "__main__":
    run_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    load_raw_jobs(run_date)