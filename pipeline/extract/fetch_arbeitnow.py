import os
import json
import requests
from datetime import datetime, timezone
from config.settings import WEBSITE_URL, PAGES_TO_RUN, STORAGE_PATH, validate
from utils.logger import get_logger


def run_fetch(run_date: str) -> list[str]:
    validate()
    logger = get_logger("fetch_arbeitnow", run_date)
    os.makedirs(STORAGE_PATH, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    files_written = []

    for page in range(1, PAGES_TO_RUN + 1):
        url = f"{WEBSITE_URL}?page={page}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        file_path = os.path.join(STORAGE_PATH, f"jobs_{run_timestamp}_page_{page}.json")
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        files_written.append(file_path)
        logger.info(f"Page {page} saved to {file_path}")

    return files_written


if __name__ == "__main__":
    run_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    run_fetch(run_date)