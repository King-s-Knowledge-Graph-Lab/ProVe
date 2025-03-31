from datetime import datetime, timezone
from functools import wraps
from flask import request
import threading
import time
from typing import Any, Union
import sys

from pymongo import MongoClient

from .utils import get_ip_location, logger, CODE_PATH

sys.path.append(CODE_PATH)
from ProVe_main_service import MongoDBHandler


class StatsDBHandler(MongoDBHandler):
    def __init__(self, connection_string="mongodb://localhost:27017/", max_retries=3):
        super().__init__(connection_string, max_retries)

    def connect(self):
        for attempt in range(self.max_retries):
            try:
                self.client = MongoClient(self.connection_string)
                self.client.server_info()
                self.db = self.client['service_usage']
                self.usage_collection = self.db['usage']
                print("Successfully connected to StatsDB")
                return
            except Exception as e:
                print(f"StatsDB connection attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    raise ConnectionError("Failed to connect to StatsDB") from e
                time.sleep(5)  # Wait before retry

    def close(self):
        """Closes the MongoDB connection."""
        if self.client:
            self.client.close()
            print("MongoDB connection closed")

    def __enter__(self):
        """Enables use with 'with' statement."""
        self.connect()
        return self  # Allows access to the instance in 'with' block

    def __exit__(self, exc_type, exc_value, traceback):
        """Ensures the connection is closed when exiting 'with' block."""
        self.close()


def log_request(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        method = request.method
        url = request.url
        headers = dict(request.headers)
        timestamp = datetime.now(timezone.utc).isoformat()

        body: Union[dict, str] = {}
        raw_body = request.get_data(as_text=True)

        if request.is_json:
            body = request.get_json()
        elif raw_body:
            body = {"raw": raw_body}

        start_time = time.monotonic()
        response = func(*args, **kwargs)
        end_time = time.monotonic()
        elapsed_time = end_time - start_time

        threading.Thread(
            target=log_usage_information,
            args=(timestamp, method, url, headers, body, elapsed_time),
            daemon=True
        ).start()

        return response
    return wrapper


def log_usage_information(
    timestamp: str,
    method: str,
    url: str,
    headers: dict[str, Any],
    body: dict[str, Any],
    elapsed_time: float
) -> None:
    try:
        with StatsDBHandler() as db:
            ip = headers.pop("X-Real-Ip", None)
            headers.pop("X-Forwarded-For", None)

            if ip:
                try:
                    headers["location"] = get_ip_location(ip)
                    headers["locations"]["hash"] = hash(ip)
                except KeyError:
                    headers["X-Real-Ip"] = ip
                    logger.error(f"when retrieving location for {ip}")
                except ConnectionError:
                    headers["X-Real-Ip"] = ip
                    logger.error("failed to retrieve location, check API")

            db.usage_collection.insert_one({
                "method": method,
                "url": url,
                "headers": headers,
                "body": body,
                "timestamp": timestamp,
                "execution_time": elapsed_time
            })
    except ConnectionError as e:
        print(f"Failed to log usage information from StatsDB: {e}")
        return
