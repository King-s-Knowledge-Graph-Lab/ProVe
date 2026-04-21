"""
Backend-agnostic contract tests for `DataStore` (ABC).

These tests are the "nothing broke after we switched backend" safety net. Each
test takes a `db` fixture that returns a `DataStore` (ABC) implementation —
today parameterised only over `MongoDBHandler`, but when `PostgreSQLHandler`
lands we add its fixture and the entire suite runs against both backends
automatically.

What's tested here:
    * Return *shapes* — not how they're computed internally.
    * Behavioural invariants that must hold regardless of backend
      (e.g. `get_latest_status_by_qid` returns None for unknown QIDs,
       `log_usage` never raises).

What's NOT tested here:
    * Backend-specific translation (e.g. "does the Mongo aggregation pipeline
      have the right `$match` stage?"). Those live in `test_mongo.py`.
"""
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from bson import ObjectId

from prove_shared.database.interface import DataStore
from prove_shared.database.mongo import MongoDBHandler


# ---------------------------------------------------------------------------
# In-memory fake Mongo collection used by the Mongo fixture
# ---------------------------------------------------------------------------
class _FakeCursor:
    def __init__(self, items):
        self._items = list(items)

    def sort(self, *_, **__):
        return self

    def __iter__(self):
        return iter(self._items)


def _apply_projection(doc, projection):
    """Shallow pymongo-style projection: `{'field': 1}` includes, `{'_id': 0}` excludes."""
    if not projection:
        return doc
    includes = {k for k, v in projection.items() if v == 1}
    excludes = {k for k, v in projection.items() if v == 0}
    if includes:
        result = {k: v for k, v in doc.items() if k in includes}
    else:
        result = dict(doc)
    for k in excludes:
        result.pop(k, None)
    return result


class _FakeCollection:
    """Seeded fake with just enough pymongo surface for the handler."""

    def __init__(self, docs=None):
        self._docs = list(docs or [])

    def find_one(self, query=None, projection=None, sort=None):
        for doc in self._docs:
            if all(doc.get(k) == v for k, v in (query or {}).items()):
                return _apply_projection(doc, projection)
        return None

    def find(self, query=None, projection=None):
        matches = [
            doc for doc in self._docs
            if all(doc.get(k) == v for k, v in (query or {}).items())
        ]
        return _FakeCursor(matches)

    def aggregate(self, pipeline):
        # Enough fidelity for contract tests: honour $match.
        match_stage = next((s["$match"] for s in pipeline if "$match" in s), {})
        matches = []
        for doc in self._docs:
            ok = True
            for k, v in match_stage.items():
                if isinstance(v, dict) and "$in" in v:
                    if doc.get(k) not in v["$in"]:
                        ok = False; break
                elif doc.get(k) != v:
                    ok = False; break
            if ok:
                matches.append(doc)
        return iter(matches)

    def update_one(self, *_, **__):
        return MagicMock(matched_count=1, modified_count=1, upserted_id=None)

    def insert_one(self, doc):
        self._docs.append(doc)
        return MagicMock(inserted_id=ObjectId())

    def find_one_and_update(self, *_, **__):
        return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def mongo_backend():
    """A `MongoDBHandler` wired to fake in-memory collections."""
    handler = MongoDBHandler.__new__(MongoDBHandler)
    handler.html_collection = _FakeCollection([
        {"task_id": "t1", "reference_id": "r1", "url": "https://a.test"},
        {"task_id": "t1", "reference_id": "r2", "url": "https://b.test"},
    ])
    handler.entailment_collection = _FakeCollection([
        {"task_id": "t1", "reference_id": "r1", "result": "SUPPORTS", "text_entailment_score": 0.9},
        {"task_id": "t1", "reference_id": "r2", "result": "REFUTES", "text_entailment_score": 0.7},
    ])
    handler.stats_collection = _FakeCollection([
        {"task_id": "t1", "entity_id": "Q42", "total_claims": 12},
    ])
    handler.status_collection = _FakeCollection([
        {"qid": "Q42", "task_id": "t1", "requested_timestamp": datetime(2024, 1, 2)},
        {"qid": "Q42", "task_id": "t0", "requested_timestamp": datetime(2024, 1, 1)},
    ])
    handler.summary_collection = _FakeCollection([
        {"_id": "Q42", "proveScore": 0.8},
    ])
    handler.user_collection = _FakeCollection([])
    handler.random_collection = _FakeCollection([])

    handler._queues = {
        "user": handler.user_collection,
        "random": handler.random_collection,
        "status": handler.status_collection,
    }
    handler._usage_db_prod = MagicMock()
    handler._usage_db_prod.__getitem__.return_value = _FakeCollection([])
    handler._usage_db_dev = MagicMock()
    handler._usage_db_dev.__getitem__.return_value = _FakeCollection([])
    handler.client = MagicMock()
    return handler


# The `params` list is the extension point: when PostgreSQLHandler is
# implemented, add its fixture id here and every test below runs against both.
@pytest.fixture(params=["mongo"])
def db(request, mongo_backend):
    """Parametrised backend fixture — extend with 'postgres' once implemented."""
    if request.param == "mongo":
        return mongo_backend
    raise NotImplementedError(f"No fixture for backend {request.param!r}")


# ===========================================================================
# Contract: every implementation satisfies DataStore (ABC)
# ===========================================================================
def test_backend_implements_datastore(db):
    """Every backend must be a DataStore (ABC) instance."""
    assert isinstance(db, DataStore)


# ===========================================================================
# Contract: reads return expected shape
# ===========================================================================
class TestStatusReads:
    def test_latest_status_returns_dict_or_none(self, db):
        assert db.get_latest_status_by_qid("Q42") is not None
        assert db.get_latest_status_by_qid("Q_missing") is None

    def test_latest_status_has_expected_keys(self, db):
        doc = db.get_latest_status_by_qid("Q42")
        assert "qid" in doc
        assert "task_id" in doc

    def test_statuses_returns_list(self, db):
        result = db.get_statuses_by_qid("Q42")
        assert isinstance(result, list)
        assert len(result) == 2

    def test_statuses_empty_for_unknown_qid(self, db):
        assert db.get_statuses_by_qid("Q_missing") == []


class TestHtmlReads:
    def test_returns_list(self, db):
        rows = db.get_html_by_task_id("t1")
        assert isinstance(rows, list)
        assert len(rows) == 2

    def test_empty_for_unknown_task(self, db):
        assert db.get_html_by_task_id("t_missing") == []


class TestEntailmentReads:
    def test_by_task_and_reference_returns_list(self, db):
        rows = db.get_entailments_by_task_and_reference("t1", "r1")
        assert isinstance(rows, list)
        assert all(r["reference_id"] == "r1" for r in rows)

    def test_aggregate_returns_list(self, db):
        result = db.aggregate_entailments_by_task_id("t1", ["r1", "r2"])
        assert isinstance(result, list)


class TestSummaryAndStatsReads:
    def test_summary_roundtrip_shape(self, db):
        assert db.get_summary_by_id("Q42") == {"_id": "Q42", "proveScore": 0.8}
        assert db.get_summary_by_id("Q_missing") is None

    def test_parser_stats_default_projection(self, db):
        assert db.get_parser_stats_by_task_and_entity("t1", "Q42") == {"total_claims": 12}

    def test_parser_stats_none_for_unknown_pair(self, db):
        assert db.get_parser_stats_by_task_and_entity("t_missing", "Q42") is None


class TestQueueReads:
    def test_get_queue_items_returns_list(self, db):
        assert db.get_queue_items("user") == []

    def test_find_queue_item_returns_none_when_empty(self, db):
        assert db.find_queue_item_by_qid("user", "Q42") is None


# ===========================================================================
# Contract: writes don't return anything, don't raise on happy path
# ===========================================================================
class TestWrites:
    def test_enqueue_item_appends_and_is_findable(self, db):
        db.enqueue_item("user", {"qid": "Q100", "task_id": "t_new", "status": "in queue"})
        assert db.find_queue_item_by_qid("user", "Q100") is not None

    def test_upsert_summary_accepts_and_returns_none(self, db):
        result = db.upsert_summary_by_id("Q500", {"proveScore": 0.5})
        assert result is None

    def test_increment_retry_does_not_raise(self, db):
        db.increment_retry_by_id("user", "any-id")

    def test_mark_queue_item_error_does_not_raise(self, db):
        db.mark_queue_item_error_by_id("user", "any-id")

    def test_update_queue_status_does_not_raise(self, db):
        db.update_queue_status_by_task_and_qid("user", "t1", "Q42", "completed")


# ===========================================================================
# Contract: log_usage never raises
# ===========================================================================
class TestLogUsageNeverRaises:
    def test_happy_path(self, db):
        db.log_usage({"method": "GET", "url": "/api/items"})

    def test_even_when_backend_broken(self, mongo_backend):
        """Backend-specific detail but the contract belongs here."""
        broken_db = MagicMock()
        broken_db.__getitem__.return_value.insert_one.side_effect = RuntimeError("boom")
        mongo_backend._usage_db_prod = broken_db

        # Must not raise.
        mongo_backend.log_usage({"method": "GET"})
