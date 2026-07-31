# @repo: api
# @description: Flask decorators for request logging (@log_request) and API key authentication (@api_required). Usage-logging now routes through MongoDBHandler.log_usage() — no more ad-hoc StatsDBHandler subclass.
from datetime import datetime, timezone
from base64 import b64decode
from functools import wraps
from flask import request
import threading
import time
from typing import Any, Union

try:
    from utils_api import get_ip_location, logger
    from local_secrets import SOURCE, API_KEY, PRIVATE_KEY
except ImportError:
    from api.utils_api import get_ip_location, logger
    from api.local_secrets import SOURCE, API_KEY, PRIVATE_KEY

from prove_shared.database import get_database
from prove_shared.auth import AsyncAuth


# ---------------------------------------------------------------------------
# Shared database handle
# ---------------------------------------------------------------------------
# One backend instance per process is enough. `get_database()` reads the
# app's config.yaml and returns whichever implementation is configured
# (Mongo today, Postgres later, or an orchestrator that writes to both
# during migration). The per-request `with StatsDBHandler()` pattern used
# previously paid a connect cost on every HTTP hit — this avoids that.
_db = get_database()


def log_request(func):
    """
    Fire-and-forget usage logger for any API route.

    Writes the request metadata to the production usage DB on a background
    thread so the actual response latency is unaffected. Logging failures are
    swallowed inside the handler — a usage-log hiccup must never surface to
    the caller as a 500.
    """
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

        # Only log in the production environment (SOURCE is set per-deploy).
        if SOURCE != 'server':
            return response

        threading.Thread(
            target=_log_usage_information,
            args=(timestamp, method, url, headers, body, elapsed_time),
            daemon=True
        ).start()

        return response
    return wrapper


def _log_usage_information(
    timestamp: str,
    method: str,
    url: str,
    headers: dict[str, Any],
    body: dict[str, Any],
    elapsed_time: float,
) -> None:
    """
    Build a usage record and hand it to the database handler.

    The IP-geolocation enrichment can raise (KeyError on unknown IPs,
    ConnectionError if the geo API is down). We handle those here so the
    record is still written without location data rather than being dropped.
    """
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

    _db.log_usage({
        "method": method,
        "url": url,
        "headers": headers,
        "body": body,
        "timestamp": timestamp,
        "execution_time": elapsed_time,
    })


def api_required(func):
    """Reject requests that don't carry a valid AsyncAuth-signed API key."""
    @wraps(func)
    def decorator(*args, **kwargs):
        if not request.json:
            return {"message": "Please provide an API key."}, 400

        api_key = request.json.get("api_key", None)
        api_key = b64decode(api_key)
        if api_key is None or not AsyncAuth.is_valid(api_key):
            return {"message": "Please provide a valid API key."}, 403
        return func(*args, **kwargs)
    return decorator
