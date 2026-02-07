import logging
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
log_dir = "LOGLOG_DIR"
log_file_name=""

def log(layer,run_id = '12345'):
    date_str = datetime.now().strftime("%Y%m%d")
    
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file_name = os.path.join(log_dir, f"{layer}_{run_id}_{date_str}.log")
    
    logger = logging.getLogger(f"{layer}_{run_id}")
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.FileHandler(log_file_name)
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler   )
    
    return logger