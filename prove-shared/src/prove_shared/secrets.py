"""Runtime secrets loader with safe fallbacks.

This module prefers values from ``prove_shared.local_secrets`` when available,
and falls back to environment variables or sensible local defaults otherwise.
"""

from __future__ import annotations

import os
from pathlib import Path


def _default_private_key_path() -> str:
    return str(Path.home() / ".prove" / "keys" / "private_key")


try:
    from .local_secrets import API_KEY as _API_KEY
    from .local_secrets import ENDPOINT as _ENDPOINT
    from .local_secrets import LOG_FILENAME as _LOG_FILENAME
    from .local_secrets import LOG_PATH as _LOG_PATH
    from .local_secrets import PRIVATE_KEY as _PRIVATE_KEY
except ModuleNotFoundError:
    _API_KEY = os.getenv("PROVE_API_KEY", "")
    _ENDPOINT = os.getenv("PROVE_ENDPOINT", "http://localhost/api/internal/")
    _LOG_FILENAME = os.getenv("PROVE_LOG_FILENAME", "prove_shared.log")
    _LOG_PATH = os.getenv("PROVE_LOG_PATH", "./logs/prove_shared/")
    _PRIVATE_KEY = os.getenv("PROVE_PRIVATE_KEY", _default_private_key_path())


API_KEY = _API_KEY
ENDPOINT = _ENDPOINT
LOG_FILENAME = _LOG_FILENAME
LOG_PATH = _LOG_PATH
PRIVATE_KEY = _PRIVATE_KEY
