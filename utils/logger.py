import logging
from logging.handlers import TimedRotatingFileHandler
import os

LOG_PATH = "/home/ubuntu/RQV/logs/"
LOG_FILENAME = "ProVe.log"

if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH, exist_ok=True)

logger = logging.getLogger("ProVe")
logger.setLevel(logging.ERROR)

handler = TimedRotatingFileHandler(LOG_PATH + LOG_FILENAME, when="midnight", interval=1, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)
