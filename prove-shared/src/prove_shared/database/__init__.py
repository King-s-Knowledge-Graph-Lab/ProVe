# @repo: shared
# @description: Public surface of the database subpackage.
"""
Database abstraction layer for ProVe.

Application code should only import from this module:

    from prove_shared.database import get_database

    db = get_database()   # returns a DatabaseInterface implementation
    db.get_latest_status_by_qid("Q42")

Which backend `db` actually is (Mongo, Postgres, or an orchestrator wrapping
both for dual-write migrations) is decided by the `database:` block in the
app's `config.yaml`. See `orchestrator.py` for the config schema.
"""
from .interface import DatabaseInterface
from .orchestrator import (
    DatabaseOrchestrator,
    get_database,
    reset_cached_database,
)
from .postgres import PostgreSQLHandler

__all__ = [
    "DatabaseInterface",
    "DatabaseOrchestrator",
    "PostgreSQLHandler",
    "get_database",
    "reset_cached_database",
]
