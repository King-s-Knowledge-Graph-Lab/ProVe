import logging
from logging.handlers import TimedRotatingFileHandler
import os

from local_secrets import API_KEY, LOG_PATH, LOG_FILENAME


if not os.path.exists(LOG_PATH):
    try:
        os.makedirs(LOG_PATH, exist_ok=True)
    except PermissionError:
        LOG_PATH = "./logs/api/"
        os.makedirs(LOG_PATH, exist_ok=True)

logger = logging.getLogger("api")
logger.setLevel(logging.ERROR)

handler = TimedRotatingFileHandler(LOG_PATH + LOG_FILENAME, when="midnight", interval=1, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)


def get_ip_location(ip: str) -> None:
    from urllib.request import urlopen
    import json
    url = f'https://geolocation-db.com/json/{API_KEY}/' + ip
    res = urlopen(url)

    if res is None:
        raise ConnectionError()

    data = json.load(res)

    if "country_name" not in data.keys():
        raise KeyError()

    return {
        "country_code": data.get("country_code", None),
        "country_name": data.get("country_name", None),
        "city": data.get("city", None),
        "state": data.get("state", None),
        "latitude": data.get("latitude", None),
        "longitude": data.get("longitude", None),
        "hash": hash(ip)
    }
