import logging
import os
from datetime import datetime
from config.settings import LOG_DIR

def get_logger(layer:str,run_id:str):

    os.makedirs(LOG_DIR,exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(LOG_DIR, f"{layer}_{run_id}_{date_str}.log")

    logger = logging.getLogger(f"{layer}_{run_id}")
    logger.setLevel(logging.INFO)

    if not logger.handlers:  
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

    return logger