"""
Tests for the `PostgreSQLHandler` stub.

The stub is a placeholder — every method must raise `NotImplementedError` so
callers fail loudly instead of silently doing nothing. Once real SQL starts
landing, the individual `test_*_raises_not_implemented` tests will flip to
real behavioural tests one at a time.

Separately, this suite proves the stub still *satisfies* `DatabaseInterface`,
so the orchestrator can legitimately construct one.
"""
from datetime import datetime

import pandas as pd
import pytest

from prove_shared.database.interface import DatabaseInterface
from prove_shared.database.postgres import PostgreSQLHandler


@pytest.fixture
def pg() -> PostgreSQLHandler:
    """Fresh stub; stub doesn't open any real connection in __init__."""
    return PostgreSQLHandler()


# ===========================================================================
# Structural: stub implements the interface
# ===========================================================================
def test_stub_is_a_database_interface(pg):
    """Proves the ABC constraints are satisfied (no missing @abstractmethod)."""
    assert isinstance(pg, DatabaseInterface)


def test_stub_carries_connection_params(pg):
    assert pg.dsn == "postgresql://localhost/prove"
    assert pg.max_retries == 3


def test_custom_dsn_and_retries():
    pg = PostgreSQLHandler(dsn="postgresql://h/d", max_retries=5)
    assert pg.dsn == "postgresql://h/d"
    assert pg.max_retries == 5


# ===========================================================================
# Behavioural: every method raises NotImplementedError with a clear message
# ===========================================================================
# The parametrisation pattern means each failure will show the exact method
# name in the pytest output — easy to spot when someone implements a method
# and forgets to update the test.

_READ_CASES = [
    ("ensure_connection", ()),
    ("get_latest_status_by_qid", ("Q42",)),
    ("get_statuses_by_qid", ("Q42",)),
    ("get_html_by_task_id", ("t1",)),
    ("get_entailments_by_task_and_reference", ("t1", "r1")),
    ("aggregate_entailments_by_task_id", ("t1", ["r1"])),
    ("get_summary_by_id", ("Q42",)),
    ("get_parser_stats_by_task_and_entity", ("t1", "Q42")),
    ("get_queue_items", ("user",)),
    ("find_queue_item_by_qid", ("user", "Q42")),
    ("get_usage_records", ()),
    ("get_next_request", (object(),)),
    ("get_request_by_id_and_reset", (object(), "some-id")),
    ("get_request_by_id", (object(), "some-id")),
    ("get_request_by_taskid", (object(), "t1")),
    ("get_all_request_in_progress", (object(),)),
]


_WRITE_CASES = [
    ("save_html_content", (pd.DataFrame(),)),
    ("save_entailment_results", (pd.DataFrame(),)),
    ("save_parser_stats", ({"entity_id": "Q42", "task_id": "t1"},)),
    ("save_status", ({"task_id": "t1", "qid": "Q42"},)),
    ("upsert_summary_by_id", ("Q42", {"proveScore": 0.5})),
    ("enqueue_item", ("user", {"qid": "Q42"})),
    ("log_usage", ({"method": "GET"},)),
    ("increment_retry_by_id", ("user", "item-1")),
    ("mark_queue_item_error_by_id", ("user", "item-1")),
    ("update_queue_status_by_task_and_qid", ("user", "t1", "Q42", "completed")),
    (
        "set_request_status_and_processing_time",
        (object(), "completed", datetime(2024, 1, 1), "some-id"),
    ),
]


@pytest.mark.parametrize("method_name,args", _READ_CASES + _WRITE_CASES)
def test_every_method_raises_not_implemented(pg, method_name, args):
    """Each stub method raises with a message pointing at itself."""
    with pytest.raises(NotImplementedError) as exc_info:
        getattr(pg, method_name)(*args)
    assert f"PostgreSQLHandler.{method_name}" in str(exc_info.value)
