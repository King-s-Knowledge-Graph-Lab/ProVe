from datetime import datetime, timezone
from base64 import b64encode, b64decode
from functools import wraps
from flask import request
import os
import threading
import time
from typing import Any, Union
import sys

from pymongo import MongoClient

from utils_api import get_ip_location, logger
from local_secrets import CODE_PATH, SOURCE, API_KEY, PRIVATE_KEY

sys.path.append(CODE_PATH)
from utils.mongo_handler import MongoDBHandler
from utils.auth import AsyncAuth


class StatsDBHandler(MongoDBHandler):
    def __init__(self, connection_string="mongodb://localhost:27017/", max_retries=3):
        super().__init__(connection_string, max_retries)

    def connect(self, max_retries: int, connection_string: str):
        for attempt in range(self.max_retries):
            try:
                self.client = MongoClient(self.connection_string)
                self.client.server_info()
                self.db = self.client['service_usage']
                self.usage_collection = self.db['usage']
                print("Successfully connected to StatsDB")
                return True
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

        if SOURCE != 'server':
            return response

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


def api_required(func):
    @wraps(func)
    def decorator(*args, **kwargs):
        if not request.json:
            return {"message": "Please provide an API key."}, 400

        api_key = request.json.get("api_key", None)
        api_key = b64decode(api_key)
        if api_key is None or not AsyncAuth.is_valid(api_key):
            return {"message": "Please provide a valid API key."}, 403
        else:
            return func(*args, **kwargs)
    return decorator

