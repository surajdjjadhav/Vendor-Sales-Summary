import logging
from logging.handlers import RotatingFileHandler
import os
from from_root import from_root
from datetime import datetime


LOG_DIR = "logs"
LOG_FILE = f"{datetime.now().strftime('%m_%d_%d_%H_%M_%S')}.log"
MAX_LOG_SIZE = 5 * 1024 * 1024
BACKUP_COUNT = 5


log_dir_path = os.path.join(from_root(), LOG_DIR)
os.makedirs(log_dir_path, exist_ok=True)
log_file_path = os.path.join(log_dir_path, LOG_FILE)


def config_logger():
    "configure logger with rotating file handler"

    logger = logging.getLogger("etl_logger")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "[%(asctime)s] %(name)s - %(levelname)s - %(message)s"
        )

        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=MAX_LOG_SIZE,
            backupCount=BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


# configuring logger

logger = config_logger()
