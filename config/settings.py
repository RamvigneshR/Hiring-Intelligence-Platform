import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent #baseconfig is written here such that this path can be used in any file to get the base diectory

ENV = os.getenv("ENV", "dev")

env_path = BASE_DIR / f".env.{ENV}"
load_dotenv(dotenv_path=env_path, override=True)

# Database Configuration
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

WEBSITE_URL = os.getenv("WEBSITE_URL")
PAGES_TO_RUN = int(os.getenv("PAGES_TO_RUN", "1"))
STORAGE_PATH = os.getenv("STORAGE_PATH", str(BASE_DIR / "arbeitnow_json"))
LOG_DIR = os.getenv("LOG_DIR", str(BASE_DIR / "logs"))


def validate():
    required = {
        "DB_USER": DB_USER,
        "DB_PASS": DB_PASS,
        "DB_HOST": DB_HOST,
        "DB_NAME": DB_NAME,
        "WEBSITE_URL": WEBSITE_URL,
        "STORAGE_PATH": STORAGE_PATH,
    }
    missing = [key for key, val in required.items() if not val]
    if missing:
        raise EnvironmentError(
            f"Missing required settings for ENV={ENV}: {missing}"
        )