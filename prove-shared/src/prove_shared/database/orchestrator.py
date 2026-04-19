# @repo: shared
# @description: Config-driven database orchestrator. Selects primary/fallback backends from YAML and exposes the same DatabaseInterface so callers are backend-agnostic.
"""
DatabaseOrchestrator — chooses and wraps the active backend.

The orchestrator is a thin, transparent wrapper around a primary
`DatabaseInterface` implementation, with two optional behaviours:

    1. Dual-write mode  (for migration) — writes go to the primary
       *and* the fallback. Writes to the fallback are log-and-continue:
       a failure there never breaks the primary write, because during
       migration the primary is still the source of truth.

    2. Read fallback    (opt-in) — if a read against the primary raises,
       try the fallback before re-raising. Off by default because a silent
       fallback on reads hides real backend outages.

Config shape (loaded from the app's existing `config.yaml`):

    database:
      primary: mongo            # "mongo" | "postgres"
      fallback: none            # "none" | "mongo" | "postgres"
      mode: single              # "single" | "dual-write"
      auto_fallback_on_read: false

      mongo:
        connection_string: mongodb://localhost:27017/
        max_retries: 3
      postgres:
        dsn: postgresql://localhost/prove
        max_retries: 3

Migration phases (same settings, different values):

    Day 1           primary=mongo,     fallback=none,     mode=single
    Migration       primary=mongo,     fallback=postgres, mode=dual-write
    Cutover         primary=postgres,  fallback=mongo,    mode=single, auto_fallback_on_read=true
    Done            primary=postgres,  fallback=none,     mode=single
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

from ..logger import logger
from .interface import DatabaseInterface, QueueRef


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
# `get_database()` is called once per process in most cases (module-level in
# each caller). We cache the result so repeated calls don't reopen connections.
# Tests can pass `config=...` explicitly to bypass the cache.
_CACHED_DB: Optional[DatabaseInterface] = None


# ---------------------------------------------------------------------------
# Backend factory
# ---------------------------------------------------------------------------
def _build_backend(kind: str, settings: Dict[str, Any]) -> DatabaseInterface:
    """
    Construct a concrete `DatabaseInterface` implementation.

    Kept here rather than in the backend modules themselves so that the
    config-shape-to-constructor-arg mapping lives in one place. Adding a new
    backend means one extra branch here and a new file alongside mongo.py /
    postgres.py.
    """
    if kind == "mongo":
        # Imported lazily so `PostgreSQLHandler`-only environments don't need
        # pymongo on the path (useful for future slimmed-down deployments).
        from ..mongo_handler import MongoDBHandler

        return MongoDBHandler(
            connection_string=settings.get(
                "connection_string", "mongodb://localhost:27017/"
            ),
            max_retries=settings.get("max_retries", 3),
        )
    if kind == "postgres":
        from .postgres import PostgreSQLHandler

        return PostgreSQLHandler(
            dsn=settings.get("dsn", "postgresql://localhost/prove"),
            max_retries=settings.get("max_retries", 3),
        )
    raise ValueError(f"Unknown database backend: {kind!r}")


class DatabaseOrchestrator(DatabaseInterface):
    """
    Routes reads and writes between a primary and an optional fallback.

    Implements `DatabaseInterface` itself so callers hold a single object
    and never know which backend is serving any given call. Method bodies
    are deliberately explicit (rather than `__getattr__`-based) so IDEs,
    type checkers, and stack traces point at the right thing.
    """

    def __init__(
        self,
        primary: DatabaseInterface,
        fallback: Optional[DatabaseInterface] = None,
        dual_write: bool = False,
        auto_fallback_on_read: bool = False,
    ) -> None:
        self.primary = primary
        self.fallback = fallback
        self.dual_write = dual_write
        self.auto_fallback_on_read = auto_fallback_on_read

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #
    def _read(self, method_name: str, *args, **kwargs):
        """
        Call `method_name` on the primary. If it fails and read-fallback is
        enabled, retry on the fallback. Otherwise re-raise.
        """
        try:
            return getattr(self.primary, method_name)(*args, **kwargs)
        except Exception as e:
            if self.auto_fallback_on_read and self.fallback is not None:
                logger.warning(
                    f"Primary read '{method_name}' failed ({e}); "
                    f"falling back to secondary backend."
                )
                return getattr(self.fallback, method_name)(*args, **kwargs)
            raise

    def _write(self, method_name: str, *args, **kwargs) -> None:
        """
        Always write to the primary. In dual-write mode, also write to the
        fallback with log-and-continue semantics — during migration the
        primary is source of truth and a fallback failure must not break
        the user-facing write.
        """
        getattr(self.primary, method_name)(*args, **kwargs)

        if self.dual_write and self.fallback is not None:
            try:
                getattr(self.fallback, method_name)(*args, **kwargs)
            except Exception as e:
                # Log and move on — dual-write is best-effort by design.
                # TODO: once Postgres is the primary, revisit this policy —
                # we may want stricter behaviour (reconciliation job, alert).
                logger.error(
                    f"Dual-write of '{method_name}' to fallback failed: {e}. "
                    "Primary succeeded; continuing."
                )

    # ==================================================================
    # Connection lifecycle
    # ==================================================================
    def ensure_connection(self, try_reconnect: bool = True) -> None:
        self.primary.ensure_connection(try_reconnect=try_reconnect)
        # Don't fail the caller just because the fallback is down — that
        # would defeat the point of having a fallback.
        if self.fallback is not None:
            try:
                self.fallback.ensure_connection(try_reconnect=try_reconnect)
            except Exception as e:
                logger.warning(f"Fallback connection check failed: {e}")

    # ==================================================================
    # Reads — simple delegations through `_read`
    # ==================================================================
    def get_latest_status_by_qid(self, qid: str) -> Optional[Dict[str, Any]]:
        return self._read("get_latest_status_by_qid", qid)

    def get_statuses_by_qid(
        self,
        qid: str,
        sort_by: Optional[str] = None,
        descending: bool = True,
    ) -> List[Dict[str, Any]]:
        return self._read("get_statuses_by_qid", qid, sort_by=sort_by, descending=descending)

    def get_html_by_task_id(
        self,
        task_id: str,
        fields: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        return self._read("get_html_by_task_id", task_id, fields=fields)

    def get_entailments_by_task_and_reference(
        self,
        task_id: str,
        reference_id: str,
    ) -> List[Dict[str, Any]]:
        return self._read("get_entailments_by_task_and_reference", task_id, reference_id)

    def aggregate_entailments_by_task_id(
        self,
        task_id: str,
        reference_ids: List[str],
    ) -> List[Dict[str, Any]]:
        return self._read("aggregate_entailments_by_task_id", task_id, reference_ids)

    def get_summary_by_id(self, target_id: str) -> Optional[Dict[str, Any]]:
        return self._read("get_summary_by_id", target_id)

    def get_parser_stats_by_task_and_entity(
        self,
        task_id: str,
        entity_id: str,
        fields: Optional[Dict[str, int]] = None,
    ) -> Optional[Dict[str, Any]]:
        return self._read(
            "get_parser_stats_by_task_and_entity",
            task_id, entity_id, fields=fields,
        )

    def get_queue_items(
        self,
        queue_name: QueueRef,
        status: Optional[str] = None,
        sort_by: Optional[str] = None,
        ascending: bool = True,
    ) -> List[Dict[str, Any]]:
        return self._read(
            "get_queue_items",
            queue_name, status=status, sort_by=sort_by, ascending=ascending,
        )

    def find_queue_item_by_qid(
        self,
        queue_name: QueueRef,
        qid: str,
    ) -> Optional[Dict[str, Any]]:
        return self._read("find_queue_item_by_qid", queue_name, qid)

    def get_usage_records(self, use_dev_db: bool = False) -> List[Dict[str, Any]]:
        return self._read("get_usage_records", use_dev_db=use_dev_db)

    # ==================================================================
    # Writes — routed through `_write`
    # ==================================================================
    def save_html_content(self, html_df: pd.DataFrame) -> None:
        self._write("save_html_content", html_df)

    def save_entailment_results(self, entailment_df: pd.DataFrame) -> None:
        self._write("save_entailment_results", entailment_df)

    def save_parser_stats(self, stats_dict: Dict[str, Any]) -> None:
        self._write("save_parser_stats", stats_dict)

    def save_status(
        self,
        status_dict: Dict[str, Any],
        queue: Optional[QueueRef] = None,
    ) -> None:
        self._write("save_status", status_dict, queue=queue)

    def upsert_summary_by_id(self, target_id: str, data: Dict[str, Any]) -> None:
        self._write("upsert_summary_by_id", target_id, data)

    def enqueue_item(self, queue_name: QueueRef, item: Dict[str, Any]) -> None:
        self._write("enqueue_item", queue_name, item)

    def log_usage(self, record: Dict[str, Any]) -> None:
        # Interface contract: log_usage must never raise. The wrapped
        # implementations already swallow errors, but wrap belt-and-braces
        # because dual-write could surface a fallback error here too.
        try:
            self._write("log_usage", record)
        except Exception as e:
            logger.error(f"Orchestrator log_usage swallowed error: {e}")

    # ==================================================================
    # Queue-state mutations
    # ==================================================================
    def increment_retry_by_id(
        self,
        queue_name: QueueRef,
        item_id: Any,
    ) -> None:
        self._write("increment_retry_by_id", queue_name, item_id)

    def mark_queue_item_error_by_id(
        self,
        queue_name: QueueRef,
        item_id: Any,
        error_message: str = "Max retry limit reached",
    ) -> None:
        self._write(
            "mark_queue_item_error_by_id",
            queue_name, item_id, error_message=error_message,
        )

    def update_queue_status_by_task_and_qid(
        self,
        queue_name: QueueRef,
        task_id: str,
        qid: str,
        status: str,
    ) -> None:
        self._write(
            "update_queue_status_by_task_and_qid",
            queue_name, task_id, qid, status,
        )

    # ==================================================================
    # Workflow primitives
    # ==================================================================
    def get_next_request(self, queue: Any) -> Optional[Dict[str, Any]]:
        # Side effect: this method both reads *and* writes (claims the row).
        # Treat it as a write — dual-write would double-claim, which is wrong,
        # so we intentionally only hit the primary here.
        return self.primary.get_next_request(queue)

    def get_request_by_id_and_reset(
        self,
        queue: Any,
        _id: str,
    ) -> Optional[Dict[str, Any]]:
        # Same rationale as `get_next_request` — primary only.
        return self.primary.get_request_by_id_and_reset(queue, _id)

    def set_request_status_and_processing_time(
        self,
        queue: Any,
        status: str,
        processing_time,
        _id: str,
    ) -> Optional[Dict[str, Any]]:
        return self.primary.set_request_status_and_processing_time(
            queue, status, processing_time, _id,
        )

    def get_request_by_id(self, queue: Any, _id: str) -> Optional[Dict[str, Any]]:
        return self._read("get_request_by_id", queue, _id)

    def get_request_by_taskid(self, queue: Any, task_id: str) -> Optional[Dict[str, Any]]:
        return self._read("get_request_by_taskid", queue, task_id)

    def get_all_request_in_progress(self, queue: Any) -> Any:
        return self._read("get_all_request_in_progress", queue)

    # ==================================================================
    # Convenience pass-throughs for Mongo-specific attributes
    # ==================================================================
    # Several legacy callers reach into `.user_collection` / `.random_collection`
    # on the handler (these are pymongo Collection objects). We expose them
    # here as a thin proxy to the primary so the transition is invisible.
    # Postgres code paths won't have these attributes; callers that still use
    # them are on the "things to migrate to queue_name-based API" list.
    def __getattr__(self, name: str) -> Any:
        # Only invoked when the attribute isn't found the normal way, so our
        # method definitions above always win.
        return getattr(self.primary, name)


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def _load_database_config(config_path: str) -> Dict[str, Any]:
    """Read and return just the `database:` block from the given YAML file."""
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg.get("database", {}) or {}


def _resolve_config_path(config_path: Optional[str]) -> str:
    """
    Decide which config.yaml to load.

    Priority:
        1. Explicit `config_path` argument.
        2. `PROVE_CONFIG_PATH` env var (used in tests / containers).
        3. `config.yaml` in the current working directory (matches existing
           behaviour of both prove-api and prove-processing).
    """
    if config_path:
        return config_path
    env_path = os.environ.get("PROVE_CONFIG_PATH")
    if env_path:
        return env_path
    return "config.yaml"


def get_database(
    config_path: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    refresh: bool = False,
) -> DatabaseInterface:
    """
    Return the configured database handler.

    Args:
        config_path: Path to a YAML file with a `database:` block. Ignored if
            `config` is provided.
        config: Inline `database:` config dict — useful for tests, skips the
            module-level cache.
        refresh: Rebuild the cached instance even if one already exists.

    Returns:
        A `DatabaseInterface` implementation. When `fallback` is None and
        `mode == "single"`, the concrete backend is returned directly (no
        orchestrator wrapper) so callers pay zero extra cost. As soon as
        fallback/dual-write is configured, the orchestrator kicks in.
    """
    global _CACHED_DB

    # Inline config is never cached — intended for tests/one-offs.
    if config is not None:
        return _build_from_config(config)

    if _CACHED_DB is not None and not refresh:
        return _CACHED_DB

    try:
        db_cfg = _load_database_config(_resolve_config_path(config_path))
    except FileNotFoundError:
        # Sensible default for fresh checkouts: single Mongo backend at localhost.
        # Matches the current default used throughout the codebase so nothing
        # regresses if someone forgets to add the `database:` block.
        logger.warning(
            "config.yaml not found or has no `database:` block; "
            "defaulting to single Mongo backend."
        )
        db_cfg = {}

    _CACHED_DB = _build_from_config(db_cfg)
    return _CACHED_DB


def _build_from_config(db_cfg: Dict[str, Any]) -> DatabaseInterface:
    """Translate a `database:` config dict into a concrete backend/orchestrator."""
    primary_kind = db_cfg.get("primary", "mongo")
    fallback_kind = db_cfg.get("fallback", "none")
    mode = db_cfg.get("mode", "single")
    auto_fallback_on_read = bool(db_cfg.get("auto_fallback_on_read", False))

    primary_settings = db_cfg.get(primary_kind, {}) or {}
    primary = _build_backend(primary_kind, primary_settings)

    # Fast path: no fallback, single-mode → skip the orchestrator entirely.
    if fallback_kind in (None, "none", "") and mode == "single" and not auto_fallback_on_read:
        return primary

    fallback: Optional[DatabaseInterface] = None
    if fallback_kind not in (None, "none", ""):
        fallback_settings = db_cfg.get(fallback_kind, {}) or {}
        fallback = _build_backend(fallback_kind, fallback_settings)

    return DatabaseOrchestrator(
        primary=primary,
        fallback=fallback,
        dual_write=(mode == "dual-write"),
        auto_fallback_on_read=auto_fallback_on_read,
    )


def reset_cached_database() -> None:
    """Clear the module-level cache. Intended for tests."""
    global _CACHED_DB
    _CACHED_DB = None
