# @repo: shared
# @description: PostgreSQL implementation of DataStore (ABC). Stub — all methods raise NotImplementedError until the migration work begins.
"""
PostgreSQLHandler — placeholder implementation of `DataStore` (ABC).

This class exists so the interface is provably pluggable today: the
orchestrator can be constructed with it, tests can mock it, and future work
can fill in one method at a time without touching the interface or the
Mongo implementation.

Every method raises `NotImplementedError` with a short hint about what the
Postgres equivalent should do. When a method is implemented, keep the
signature identical to the interface — no extra params, no different
return shape.

Intended dependencies (added to `pyproject.toml` when we start
implementation):
    * `psycopg[binary]>=3.1`  or  `psycopg2-binary>=2.9`
    * `SQLAlchemy>=2.0`       (optional, for migrations/models)
    * `alembic>=1.13`         (schema migration tool)

Schema TODOs (to be defined before any method is implemented):
    * `status(task_id PK, qid, status, algo_version, request_type,
              requested_timestamp, processing_start_timestamp,
              completed_timestamp, last_updated)`
    * `html_content(reference_id, task_id, url, lang, status, save_timestamp,
                    fetch_timestamp, entity_label, property_label,
                    object_label, object_id, property_id,
                    PRIMARY KEY(reference_id, task_id))`
    * `entailment_results(id PK, reference_id, task_id, result,
                          text_entailment_score, result_sentence,
                          processed_timestamp, save_timestamp)`
    * `parser_stats(task_id, entity_id, total_claims, parsing_start_timestamp,
                    save_timestamp, PRIMARY KEY(task_id, entity_id))`
    * `summary(id PK, algo_version, last_update, status, total_claims,
               prove_score, count_refuting, count_inconclusive,
               count_supportive, count_irretrievable)`
    * `queue_user` / `queue_random` / `queue_status` — same columns as
      `status`, separate tables so indexes and concurrency characteristics
      can diverge if needed.
    * `usage_log(id PK, method, url, headers JSONB, body JSONB, timestamp,
                 execution_time)` — one table; prod vs dev mirror is a
                 schema or an env switch, not a separate DB.

General notes for whoever writes the real thing:
    * All `upsert_*` methods map to `INSERT ... ON CONFLICT (...) DO UPDATE`.
    * `$inc` maps to `UPDATE ... SET col = col + 1`.
    * `aggregate_entailments_by_task_id` can be one query with
      `ROW_NUMBER() OVER (PARTITION BY reference_id, result ORDER BY score DESC)`
      plus filtering to keep the "top per bucket".
    * `get_next_request` needs `FOR UPDATE SKIP LOCKED` to match the atomic
      claim semantics of the current Mongo `find_one_and_update`.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from .interface import DataStore, QueueRef


def _not_implemented(method_name: str) -> NotImplementedError:
    """Small helper so the message style stays consistent across methods."""
    return NotImplementedError(
        f"PostgreSQLHandler.{method_name} is not implemented yet. "
        "See module docstring for schema and implementation notes."
    )


class PostgreSQLHandler(DataStore):
    """Stub Postgres backend. Populate one method at a time."""

    def __init__(
        self,
        dsn: str = "postgresql://localhost/prove",
        max_retries: int = 3,
    ) -> None:
        # TODO: open a psycopg / SQLAlchemy connection pool using `dsn`.
        # Keep `max_retries` semantics consistent with MongoDBHandler so the
        # orchestrator's retry behaviour is identical across backends.
        self.dsn = dsn
        self.max_retries = max_retries

    # ------------------------------------------------------------------ #
    # Connection lifecycle                                               #
    # ------------------------------------------------------------------ #
    def ensure_connection(self, try_reconnect: bool = True) -> None:
        raise _not_implemented("ensure_connection")

    # ---- Reads -------------------------------------------------------- #
    def get_latest_status_by_qid(self, qid: str) -> Optional[Dict[str, Any]]:
        # TODO: SELECT * FROM status WHERE qid = %s ORDER BY requested_timestamp DESC LIMIT 1
        raise _not_implemented("get_latest_status_by_qid")

    def get_statuses_by_qid(
        self,
        qid: str,
        sort_by: Optional[str] = None,
        descending: bool = True,
    ) -> List[Dict[str, Any]]:
        raise _not_implemented("get_statuses_by_qid")

    def get_html_by_task_id(
        self,
        task_id: str,
        fields: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        # TODO: translate Mongo projection `{'col': 1, ...}` into a SELECT column list.
        raise _not_implemented("get_html_by_task_id")

    def get_entailments_by_task_and_reference(
        self,
        task_id: str,
        reference_id: str,
    ) -> List[Dict[str, Any]]:
        raise _not_implemented("get_entailments_by_task_and_reference")

    def aggregate_entailments_by_task_id(
        self,
        task_id: str,
        reference_ids: List[str],
    ) -> List[Dict[str, Any]]:
        # TODO: window function approach — see module docstring.
        raise _not_implemented("aggregate_entailments_by_task_id")

    def get_summary_by_id(self, target_id: str) -> Optional[Dict[str, Any]]:
        raise _not_implemented("get_summary_by_id")

    def get_parser_stats_by_task_and_entity(
        self,
        task_id: str,
        entity_id: str,
        fields: Optional[Dict[str, int]] = None,
    ) -> Optional[Dict[str, Any]]:
        raise _not_implemented("get_parser_stats_by_task_and_entity")

    def get_queue_items(
        self,
        queue_name: QueueRef,
        status: Optional[str] = None,
        sort_by: Optional[str] = None,
        ascending: bool = True,
    ) -> List[Dict[str, Any]]:
        raise _not_implemented("get_queue_items")

    def find_queue_item_by_qid(
        self,
        queue_name: QueueRef,
        qid: str,
    ) -> Optional[Dict[str, Any]]:
        raise _not_implemented("find_queue_item_by_qid")

    def get_usage_records(self, use_dev_db: bool = False) -> List[Dict[str, Any]]:
        raise _not_implemented("get_usage_records")

    # ---- Writes ------------------------------------------------------- #
    def save_html_content(self, html_df: pd.DataFrame) -> None:
        raise _not_implemented("save_html_content")

    def save_entailment_results(self, entailment_df: pd.DataFrame) -> None:
        raise _not_implemented("save_entailment_results")

    def save_parser_stats(self, stats_dict: Dict[str, Any]) -> None:
        raise _not_implemented("save_parser_stats")

    def save_status(
        self,
        status_dict: Dict[str, Any],
        queue: Optional[QueueRef] = None,
    ) -> None:
        raise _not_implemented("save_status")

    def upsert_summary_by_id(self, target_id: str, data: Dict[str, Any]) -> None:
        # TODO: INSERT ... ON CONFLICT (id) DO UPDATE SET ...
        raise _not_implemented("upsert_summary_by_id")

    def enqueue_item(self, queue_name: QueueRef, item: Dict[str, Any]) -> None:
        raise _not_implemented("enqueue_item")

    def log_usage(self, record: Dict[str, Any]) -> None:
        # TODO: INSERT INTO usage_log (...) VALUES (...)
        # Remember: must swallow errors per the interface contract.
        raise _not_implemented("log_usage")

    # ---- Queue-state mutations --------------------------------------- #
    def increment_retry_by_id(
        self,
        queue_name: QueueRef,
        item_id: Any,
    ) -> None:
        # TODO: UPDATE <queue_table> SET retry_count = retry_count + 1 WHERE id = %s
        raise _not_implemented("increment_retry_by_id")

    def mark_queue_item_error_by_id(
        self,
        queue_name: QueueRef,
        item_id: Any,
        error_message: str = "Max retry limit reached",
    ) -> None:
        raise _not_implemented("mark_queue_item_error_by_id")

    def update_queue_status_by_task_and_qid(
        self,
        queue_name: QueueRef,
        task_id: str,
        qid: str,
        status: str,
    ) -> None:
        raise _not_implemented("update_queue_status_by_task_and_qid")

    # ---- Workflow primitives ----------------------------------------- #
    def get_next_request(self, queue: Any) -> Optional[Dict[str, Any]]:
        # TODO: SELECT ... FOR UPDATE SKIP LOCKED; UPDATE ... RETURNING *
        raise _not_implemented("get_next_request")

    def get_request_by_id_and_reset(
        self,
        queue: Any,
        _id: str,
    ) -> Optional[Dict[str, Any]]:
        raise _not_implemented("get_request_by_id_and_reset")

    def set_request_status_and_processing_time(
        self,
        queue: Any,
        status: str,
        processing_time: datetime,
        _id: str,
    ) -> Optional[Dict[str, Any]]:
        raise _not_implemented("set_request_status_and_processing_time")

    def get_request_by_id(self, queue: Any, _id: str) -> Optional[Dict[str, Any]]:
        raise _not_implemented("get_request_by_id")

    def get_request_by_taskid(self, queue: Any, task_id: str) -> Optional[Dict[str, Any]]:
        raise _not_implemented("get_request_by_taskid")

    def get_all_request_in_progress(self, queue: Any) -> Any:
        raise _not_implemented("get_all_request_in_progress")
