from sqlalchemy import create_engine
from config.settings import DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME

def get_engine():
    return create_engine(
        f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )