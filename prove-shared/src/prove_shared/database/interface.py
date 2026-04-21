# @repo: shared
# @description: DataStore (ABC) — the database contract. Every backend (Mongo, Postgres) must implement this so the orchestrator can swap them at runtime.
"""
DataStore (ABC) — the single contract every backend implements.

Why it exists:
    We're migrating from MongoDB to PostgreSQL (MongoDB is not compliant with
    Wikimedia's open-source requirements). The migration needs to be config-
    driven, backward-compatible, and reversible — which means application
    code must never couple to a specific backend.

    This contract is the boundary. Callers depend on `DataStore` (ABC);
    concrete implementations (`MongoDBHandler`, `PostgreSQLHandler`)
    implement it. The `DatabaseOrchestrator` wraps whichever implementation
    the YAML config selects at runtime.

Naming convention:
    * `get_<thing>_by_<key>(...)`   — single keyed lookup, returns Optional[dict]
    * `get_<things>(...)` / `get_<things>_by_<key>(...)` — list return
    * verb-first (`save_`, `log_`, `enqueue_`, `increment_`, `mark_`, `update_`)
      for mutations
    * Inputs are plain Python (str, dict, list). No BSON, no ObjectId leaks.
    * Reads return Optional[dict] or List[dict] — never a pymongo Cursor and
      never a SQLAlchemy ResultProxy — so the caller can't tell the backends
      apart.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------
# `QueueRef` accepts either a short queue name ("user" / "random" / "status")
# — the preferred, backend-agnostic form — or a raw backend object (pymongo
# Collection today, SQLAlchemy Table tomorrow). The dual-acceptance keeps
# the migration smooth; Phase 3 can narrow this to `str` once every caller
# is off raw collection references.
QueueRef = Union[str, Any]


class DataStore(ABC):
    """
    Contract every ProVe database backend must implement.

    New methods should only be added here when a caller genuinely needs a
    new primitive — not to expose incidental pymongo or SQLAlchemy features.
    Every method added here becomes a commitment the Postgres backend must
    also satisfy.
    """

    # ------------------------------------------------------------------ #
    # Connection lifecycle                                               #
    # ------------------------------------------------------------------ #
    @abstractmethod
    def ensure_connection(self, try_reconnect: bool = True) -> None:
        """Verify the backend connection is alive; reconnect if permitted."""

    # ==================================================================
    # Reads
    # ==================================================================
    @abstractmethod
    def get_latest_status_by_qid(self, qid: str) -> Optional[Dict[str, Any]]:
        """Most recent status row for `qid` (largest requested_timestamp)."""

    @abstractmethod
    def get_statuses_by_qid(
        self,
        qid: str,
        sort_by: Optional[str] = None,
        descending: bool = True,
    ) -> List[Dict[str, Any]]:
        """Every status row for `qid`, optionally sorted by `sort_by`."""

    @abstractmethod
    def get_html_by_task_id(
        self,
        task_id: str,
        fields: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """HTML-content rows for `task_id`. `fields` is an optional projection."""

    @abstractmethod
    def get_entailments_by_task_and_reference(
        self,
        task_id: str,
        reference_id: str,
    ) -> List[Dict[str, Any]]:
        """Entailment rows for the (task_id, reference_id) pair."""

    @abstractmethod
    def aggregate_entailments_by_task_id(
        self,
        task_id: str,
        reference_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Top entailments for `task_id`, restricted to `reference_ids`, grouped
        by (reference_id, result). Mongo does this server-side with `$group`;
        Postgres can do the same with window functions or a client-side fold.
        """

    @abstractmethod
    def get_summary_by_id(self, target_id: str) -> Optional[Dict[str, Any]]:
        """Cached summary for `target_id` (a QID), or None if uncomputed."""

    @abstractmethod
    def get_parser_stats_by_task_and_entity(
        self,
        task_id: str,
        entity_id: str,
        fields: Optional[Dict[str, int]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Parser-stats row keyed by (task_id, entity_id)."""

    @abstractmethod
    def get_queue_items(
        self,
        queue_name: QueueRef,
        status: Optional[str] = None,
        sort_by: Optional[str] = None,
        ascending: bool = True,
    ) -> List[Dict[str, Any]]:
        """Items from `queue_name`, optionally filtered by status and sorted."""

    @abstractmethod
    def find_queue_item_by_qid(
        self,
        queue_name: QueueRef,
        qid: str,
    ) -> Optional[Dict[str, Any]]:
        """Single queue item matching `qid`, or None."""

    @abstractmethod
    def get_usage_records(self, use_dev_db: bool = False) -> List[Dict[str, Any]]:
        """All API-usage records. `use_dev_db=True` reads the dev mirror."""

    # ==================================================================
    # Writes
    # ==================================================================
    @abstractmethod
    def save_html_content(self, html_df: pd.DataFrame) -> None:
        """Upsert HTML content rows from a DataFrame."""

    @abstractmethod
    def save_entailment_results(self, entailment_df: pd.DataFrame) -> None:
        """Insert entailment result rows from a DataFrame."""

    @abstractmethod
    def save_parser_stats(self, stats_dict: Dict[str, Any]) -> None:
        """Upsert parser-stats row keyed by (task_id, entity_id)."""

    @abstractmethod
    def save_status(
        self,
        status_dict: Dict[str, Any],
        queue: Optional[QueueRef] = None,
    ) -> None:
        """Upsert a status document, optionally in a non-default queue."""

    @abstractmethod
    def upsert_summary_by_id(self, target_id: str, data: Dict[str, Any]) -> None:
        """Atomically insert-or-update the summary document for `target_id`."""

    @abstractmethod
    def enqueue_item(self, queue_name: QueueRef, item: Dict[str, Any]) -> None:
        """Append `item` to `queue_name`."""

    @abstractmethod
    def log_usage(self, record: Dict[str, Any]) -> None:
        """
        Persist a single API-usage record to the production usage store.
        Implementations MUST swallow errors — a usage-log failure must never
        surface to the caller.
        """

    # ==================================================================
    # Queue-state mutations
    # ==================================================================
    @abstractmethod
    def increment_retry_by_id(
        self,
        queue_name: QueueRef,
        item_id: Any,
    ) -> None:
        """Atomically bump `retry_count` on the given queue item by 1."""

    @abstractmethod
    def mark_queue_item_error_by_id(
        self,
        queue_name: QueueRef,
        item_id: Any,
        error_message: str = "Max retry limit reached",
    ) -> None:
        """Flip a queue item to status='error' with a reason."""

    @abstractmethod
    def update_queue_status_by_task_and_qid(
        self,
        queue_name: QueueRef,
        task_id: str,
        qid: str,
        status: str,
    ) -> None:
        """Set the `status` field on the (task_id, qid) row in `queue_name`."""

    # ==================================================================
    # Workflow primitives (existing queue-manager API)
    # ==================================================================
    # These predate Phase 1 and already follow a reasonable shape. They are
    # part of the contract so Postgres must implement them too, but they
    # take raw backend references (pymongo Collection today). Phase 3 will
    # normalise them to queue names.

    @abstractmethod
    def get_next_request(self, queue: Any) -> Optional[Dict[str, Any]]:
        """Atomically claim the next pending item from a queue."""

    @abstractmethod
    def get_request_by_id_and_reset(
        self,
        queue: Any,
        _id: str,
    ) -> Optional[Dict[str, Any]]:
        """Return a claimed item to 'in queue' state (undo a claim)."""

    @abstractmethod
    def set_request_status_and_processing_time(
        self,
        queue: Any,
        status: str,
        processing_time: datetime,
        _id: str,
    ) -> Optional[Dict[str, Any]]:
        """Set both `status` and `processing_start_timestamp` on an item."""

    @abstractmethod
    def get_request_by_id(self, queue: Any, _id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single request by its primary key."""

    @abstractmethod
    def get_request_by_taskid(self, queue: Any, task_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single request by its `task_id`."""

    @abstractmethod
    def get_all_request_in_progress(self, queue: Any) -> Any:
        """All items currently in 'processing' on a queue."""
