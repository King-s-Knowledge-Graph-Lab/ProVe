# @repo: api
# @description: Runtime configuration for prove-api, loaded from environment variables (replaces the old gitignored local_secrets.py).
import os

# Set to "server" in production so @log_request actually writes usage records;
# any other value (e.g. "local") makes it a no-op — same behavior the old
# local_secrets.SOURCE flag had.
SOURCE = os.getenv("PROVEAPI_SOURCE", "local")

# Key for the geolocation-db.com lookup used by utils_api.get_ip_location.
# Unrelated to prove_shared's PROVE_API_KEY (that's the AsyncAuth signing key).
GEO_API_KEY = os.getenv("PROVEAPI_GEO_API_KEY", "")

LOG_PATH = os.getenv("PROVEAPI_LOG_PATH", "./logs/api/")
LOG_FILENAME = os.getenv("PROVEAPI_LOG_FILENAME", "api.log")
MAX_CONNECTIONS = int(os.getenv("PROVEAPI_MAX_CONNECTIONS", "1"))
