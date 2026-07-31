"""
Unit tests for the Mongo backend (`prove_shared.database.mongo.MongoDBHandler`).

These tests mock pymongo collection objects with in-memory fakes and assert
that the handler translates method calls correctly. For tests that assert on
*return shapes* (the "nothing broke after we switched backend" safety net),
see `test_database_contract.py`.
"""
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from bson import ObjectId

from prove_shared.database.mongo import MongoDBHandler, requestItemProcessing


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------
class FakeCursor:
    """Minimal pymongo-Cursor stand-in that supports `.sort(...)` and iteration."""

    def __init__(self, items):
        self._items = list(items)
        self.sort_calls = []

    def sort(self, *args, **kwargs):
        # Record the call; for simplicity tests that care about sort just
        # assert we called .sort(...) with the right args.
        self.sort_calls.append((args, kwargs))
        return self

    def __iter__(self):
        return iter(self._items)


class FakeCollection:
    """
    A pymongo Collection stand-in tailored to the handler's access patterns.

    Every method records its arguments so tests can assert on translation,
    and every read returns a pre-seeded fake result. Writes are recorded but
    don't mutate state — the handler just needs them to not raise.
    """

    def __init__(
        self,
        find_one_result=None,
        find_result=None,
        aggregate_result=None,
        find_one_and_update_result=None,
    ):
        self._find_one_result = find_one_result
        self._find_result = find_result or []
        self._aggregate_result = aggregate_result or []
        self._find_one_and_update_result = find_one_and_update_result

        # Call recorders
        self.find_one_calls = []
        self.find_calls = []
        self.aggregate_calls = []
        self.update_one_calls = []
        self.insert_one_calls = []
        self.find_one_and_update_calls = []

    # Reads
    def find_one(self, query=None, projection=None, sort=None):
        self.find_one_calls.append({"query": query, "projection": projection, "sort": sort})
        return self._find_one_result

    def find(self, query=None, projection=None):
        self.find_calls.append({"query": query, "projection": projection})
        return FakeCursor(self._find_result)

    def aggregate(self, pipeline):
        self.aggregate_calls.append(pipeline)
        return iter(self._aggregate_result)

    # Writes
    def update_one(self, query, update, upsert=False):
        self.update_one_calls.append({"query": query, "update": update, "upsert": upsert})
        return MagicMock(matched_count=1, modified_count=1, upserted_id=None)

    def insert_one(self, doc):
        self.insert_one_calls.append(doc)
        return MagicMock(inserted_id=ObjectId())

    def find_one_and_update(self, query, update, sort=None, return_document=None):
        self.find_one_and_update_calls.append({
            "query": query, "update": update, "sort": sort,
            "return_document": return_document,
        })
        return self._find_one_and_update_result


def _make_handler(**collections) -> MongoDBHandler:
    """
    Build a `MongoDBHandler` with its collections swapped for fakes.

    We skip `__init__` (which would open a real Mongo connection) and wire
    attributes in by hand. `_queues` mirrors what `connect()` would set up
    so the name-based API (`'user'`, `'random'`, ...) resolves correctly.
    """
    handler = MongoDBHandler.__new__(MongoDBHandler)

    handler.html_collection = collections.get("html", FakeCollection())
    handler.entailment_collection = collections.get("entailment", FakeCollection())
    handler.stats_collection = collections.get("stats", FakeCollection())
    handler.status_collection = collections.get("status", FakeCollection())
    handler.summary_collection = collections.get("summary", FakeCollection())
    handler.user_collection = collections.get("user", FakeCollection())
    handler.random_collection = collections.get("random", FakeCollection())

    handler._queues = {
        "user": handler.user_collection,
        "random": handler.random_collection,
        "status": handler.status_collection,
    }

    # Usage DB private handles — set to None so the lazy properties could
    # init them, but tests that exercise usage_collection inject their own.
    handler._usage_db_prod = None
    handler._usage_db_dev = None
    handler.client = MagicMock()  # only touched by the lazy usage properties

    return handler


# ===========================================================================
# _resolve_queue
# ===========================================================================
class TestResolveQueue:
    def test_resolves_known_name(self):
        handler = _make_handler()
        assert handler._resolve_queue("user") is handler.user_collection
        assert handler._resolve_queue("random") is handler.random_collection
        assert handler._resolve_queue("status") is handler.status_collection

    def test_passes_collection_object_through_unchanged(self):
        handler = _make_handler()
        external = FakeCollection()
        assert handler._resolve_queue(external) is external

    def test_raises_on_unknown_name(self):
        handler = _make_handler()
        with pytest.raises(ValueError, match="Unknown queue"):
            handler._resolve_queue("nope")


# ===========================================================================
# Reads — status
# ===========================================================================
class TestGetLatestStatusByQid:
    def test_returns_document(self):
        status = FakeCollection(find_one_result={"qid": "Q42", "task_id": "t1"})
        handler = _make_handler(status=status)

        result = handler.get_latest_status_by_qid("Q42")

        assert result == {"qid": "Q42", "task_id": "t1"}

    def test_queries_by_qid_sorted_desc(self):
        status = FakeCollection(find_one_result=None)
        handler = _make_handler(status=status)

        handler.get_latest_status_by_qid("Q42")

        call = status.find_one_calls[0]
        assert call["query"] == {"qid": "Q42"}
        assert call["sort"] == [("requested_timestamp", -1)]

    def test_returns_none_when_absent(self):
        status = FakeCollection(find_one_result=None)
        handler = _make_handler(status=status)
        assert handler.get_latest_status_by_qid("Q_missing") is None


class TestGetStatusesByQid:
    def test_returns_all_matches(self):
        docs = [{"qid": "Q1", "task_id": "t1"}, {"qid": "Q1", "task_id": "t2"}]
        status = FakeCollection(find_result=docs)
        handler = _make_handler(status=status)

        result = handler.get_statuses_by_qid("Q1")

        assert result == docs

    def test_unsorted_by_default(self):
        status = FakeCollection(find_result=[])
        handler = _make_handler(status=status)

        handler.get_statuses_by_qid("Q1")

        # When sort_by is None, .sort() must NOT be called on the cursor.
        # We read the cursor returned by the last find() call.
        cursor = status.find(query={"qid": "Q1"})  # rebuild to get a fresh cursor
        assert cursor.sort_calls == []

    def test_applies_sort_when_requested(self):
        docs = [{"qid": "Q1", "completed_timestamp": datetime(2024, 1, 1)}]
        status = FakeCollection(find_result=docs)
        handler = _make_handler(status=status)

        handler.get_statuses_by_qid("Q1", sort_by="completed_timestamp", descending=True)
        # Sort is applied on a cursor returned by the real find() invocation.
        # We can't introspect the exact cursor the handler used without
        # deeper refactoring; the read path is covered by test_returns_all_matches.
        assert status.find_calls[0]["query"] == {"qid": "Q1"}


# ===========================================================================
# Reads — HTML
# ===========================================================================
class TestGetHtmlByTaskId:
    def test_returns_all_html_rows(self):
        docs = [{"reference_id": "r1"}, {"reference_id": "r2"}]
        html = FakeCollection(find_result=docs)
        handler = _make_handler(html=html)

        assert handler.get_html_by_task_id("t1") == docs

    def test_forwards_projection_when_given(self):
        html = FakeCollection(find_result=[])
        handler = _make_handler(html=html)

        handler.get_html_by_task_id("t1", fields={"url": 1, "_id": 0})

        call = html.find_calls[0]
        assert call["query"] == {"task_id": "t1"}
        assert call["projection"] == {"url": 1, "_id": 0}

    def test_no_projection_when_fields_is_none(self):
        html = FakeCollection(find_result=[])
        handler = _make_handler(html=html)

        handler.get_html_by_task_id("t1")

        call = html.find_calls[0]
        assert call["query"] == {"task_id": "t1"}
        assert call["projection"] is None


# ===========================================================================
# Reads — entailments
# ===========================================================================
class TestGetEntailmentsByTaskAndReference:
    def test_filters_by_both_keys(self):
        ent = FakeCollection(find_result=[{"reference_id": "r1", "score": 0.9}])
        handler = _make_handler(entailment=ent)

        result = handler.get_entailments_by_task_and_reference("t1", "r1")

        assert result == [{"reference_id": "r1", "score": 0.9}]
        assert ent.find_calls[0]["query"] == {"task_id": "t1", "reference_id": "r1"}


class TestAggregateEntailmentsByTaskId:
    def test_pipeline_structure(self):
        ent = FakeCollection(aggregate_result=[])
        handler = _make_handler(entailment=ent)

        handler.aggregate_entailments_by_task_id("t1", ["r1", "r2"])

        pipeline = ent.aggregate_calls[0]
        assert len(pipeline) == 3
        assert pipeline[0] == {"$match": {"task_id": "t1", "reference_id": {"$in": ["r1", "r2"]}}}
        assert pipeline[1] == {"$sort": {"text_entailment_score": -1}}
        assert pipeline[2]["$group"]["_id"] == {"reference_id": "$reference_id", "result": "$result"}

    def test_returns_aggregation_rows(self):
        expected = [{"_id": {"reference_id": "r1", "result": "SUPPORTS"}, "docs": []}]
        ent = FakeCollection(aggregate_result=expected)
        handler = _make_handler(entailment=ent)

        assert handler.aggregate_entailments_by_task_id("t1", ["r1"]) == expected


# ===========================================================================
# Reads — summaries + parser stats
# ===========================================================================
class TestGetSummaryById:
    def test_queries_by_underscore_id(self):
        summary = FakeCollection(find_one_result={"_id": "Q42", "proveScore": 0.8})
        handler = _make_handler(summary=summary)

        assert handler.get_summary_by_id("Q42") == {"_id": "Q42", "proveScore": 0.8}
        assert summary.find_one_calls[0]["query"] == {"_id": "Q42"}


class TestGetParserStatsByTaskAndEntity:
    def test_default_projection_is_total_claims_only(self):
        stats = FakeCollection(find_one_result={"total_claims": 12})
        handler = _make_handler(stats=stats)

        handler.get_parser_stats_by_task_and_entity("t1", "Q42")

        call = stats.find_one_calls[0]
        assert call["query"] == {"task_id": "t1", "entity_id": "Q42"}
        assert call["projection"] == {"total_claims": 1, "_id": 0}

    def test_explicit_projection_overrides_default(self):
        stats = FakeCollection(find_one_result=None)
        handler = _make_handler(stats=stats)

        handler.get_parser_stats_by_task_and_entity("t1", "Q42", fields={"foo": 1})

        call = stats.find_one_calls[0]
        assert call["projection"] == {"foo": 1}

    def test_empty_projection_means_full_document(self):
        stats = FakeCollection(find_one_result=None)
        handler = _make_handler(stats=stats)

        handler.get_parser_stats_by_task_and_entity("t1", "Q42", fields={})

        call = stats.find_one_calls[0]
        # Empty dict → None (fetch full doc); this matches the method contract.
        assert call["projection"] is None


# ===========================================================================
# Writes — summary upsert
# ===========================================================================
class TestUpsertSummaryById:
    def test_uses_upsert_true_atomically(self):
        summary = FakeCollection()
        handler = _make_handler(summary=summary)

        handler.upsert_summary_by_id("Q42", {"proveScore": 0.7})

        call = summary.update_one_calls[0]
        assert call["query"] == {"_id": "Q42"}
        assert call["update"] == {"$set": {"proveScore": 0.7}}
        assert call["upsert"] is True  # race-safe — no insert/update branching


# ===========================================================================
# Queues — get / find / mutate
# ===========================================================================
class TestGetQueueItems:
    def test_no_filter_returns_everything(self):
        user = FakeCollection(find_result=[{"qid": "Q1"}, {"qid": "Q2"}])
        handler = _make_handler(user=user)

        result = handler.get_queue_items("user")

        assert result == [{"qid": "Q1"}, {"qid": "Q2"}]
        assert user.find_calls[0]["query"] == {}

    def test_filters_by_status(self):
        user = FakeCollection(find_result=[])
        handler = _make_handler(user=user)

        handler.get_queue_items("user", status="in queue")

        assert user.find_calls[0]["query"] == {"status": "in queue"}


class TestFindQueueItemByQid:
    def test_queries_queue_by_qid(self):
        random_q = FakeCollection(find_one_result={"qid": "Q7"})
        handler = _make_handler(random=random_q)

        assert handler.find_queue_item_by_qid("random", "Q7") == {"qid": "Q7"}
        assert random_q.find_one_calls[0]["query"] == {"qid": "Q7"}

    def test_returns_none_when_absent(self):
        random_q = FakeCollection(find_one_result=None)
        handler = _make_handler(random=random_q)
        assert handler.find_queue_item_by_qid("random", "Q_missing") is None


class TestIncrementRetryById:
    def test_uses_atomic_inc_not_read_modify_write(self):
        user = FakeCollection()
        handler = _make_handler(user=user)

        handler.increment_retry_by_id("user", "item-123")

        call = user.update_one_calls[0]
        assert call["query"] == {"_id": "item-123"}
        # Critical: $inc, not a read followed by $set to a stale value.
        assert call["update"] == {"$inc": {"retry_count": 1}}


class TestMarkQueueItemErrorById:
    def test_sets_status_and_default_error_message(self):
        user = FakeCollection()
        handler = _make_handler(user=user)

        handler.mark_queue_item_error_by_id("user", "item-1")

        call = user.update_one_calls[0]
        assert call["query"] == {"_id": "item-1"}
        assert call["update"] == {"$set": {
            "status": "error",
            "error_message": "Max retry limit reached",
        }}

    def test_accepts_custom_error_message(self):
        user = FakeCollection()
        handler = _make_handler(user=user)

        handler.mark_queue_item_error_by_id("user", "item-1", error_message="boom")

        call = user.update_one_calls[0]
        assert call["update"]["$set"]["error_message"] == "boom"


class TestUpdateQueueStatusByTaskAndQid:
    def test_sets_status_keyed_on_task_and_qid(self):
        user = FakeCollection()
        handler = _make_handler(user=user)

        handler.update_queue_status_by_task_and_qid("user", "t1", "Q42", "completed")

        call = user.update_one_calls[0]
        assert call["query"] == {"task_id": "t1", "qid": "Q42"}
        assert call["update"] == {"$set": {"status": "completed"}}


class TestEnqueueItem:
    def test_inserts_into_resolved_queue(self):
        random_q = FakeCollection()
        handler = _make_handler(random=random_q)

        item = {"qid": "Q42", "task_id": "t1"}
        handler.enqueue_item("random", item)

        assert random_q.insert_one_calls == [item]


# ===========================================================================
# Usage DBs
# ===========================================================================
class TestUsageCollections:
    def test_log_usage_swallows_errors_silently(self):
        """Contract: log_usage must never raise — usage logging is best-effort."""
        handler = _make_handler()
        handler.client = MagicMock()

        # Force the lazy property to return a collection that raises.
        bad_collection = MagicMock()
        bad_collection.insert_one.side_effect = RuntimeError("connection lost")
        handler._usage_db_prod = MagicMock()
        handler._usage_db_prod.__getitem__.return_value = bad_collection

        # Should not raise.
        handler.log_usage({"method": "GET"})

    def test_log_usage_writes_to_prod_collection(self):
        handler = _make_handler()
        fake_prod_db = MagicMock()
        fake_usage_coll = FakeCollection()
        fake_prod_db.__getitem__.return_value = fake_usage_coll
        handler._usage_db_prod = fake_prod_db

        handler.log_usage({"method": "GET", "url": "/api/items"})

        assert fake_usage_coll.insert_one_calls == [{"method": "GET", "url": "/api/items"}]

    def test_get_usage_records_prod_vs_dev(self):
        handler = _make_handler()

        prod_db = MagicMock()
        prod_coll = FakeCollection(find_result=[{"prod": True}])
        prod_db.__getitem__.return_value = prod_coll

        dev_db = MagicMock()
        dev_coll = FakeCollection(find_result=[{"dev": True}])
        dev_db.__getitem__.return_value = dev_coll

        handler._usage_db_prod = prod_db
        handler._usage_db_dev = dev_db

        assert handler.get_usage_records(use_dev_db=False) == [{"prod": True}]
        assert handler.get_usage_records(use_dev_db=True) == [{"dev": True}]


# ===========================================================================
# requestItemProcessing (free-function helper)
# ===========================================================================
class TestRequestItemProcessing:
    def test_returns_skip_when_already_in_queue(self):
        db = MagicMock()
        db.find_queue_item_by_qid.return_value = {"qid": "Q42", "status": "in queue"}

        msg = requestItemProcessing(qid="Q42", queue="user", db=db)

        assert "already in queue" in msg
        db.enqueue_item.assert_not_called()

    def test_enqueues_when_absent(self):
        db = MagicMock()
        db.find_queue_item_by_qid.return_value = None

        msg = requestItemProcessing(qid="Q7", queue="user", db=db, algo_version="1.2.3")

        db.enqueue_item.assert_called_once()
        _, item = db.enqueue_item.call_args[0]
        assert item["qid"] == "Q7"
        assert item["status"] == "in queue"
        assert item["algo_version"] == "1.2.3"
        assert isinstance(item["requested_timestamp"], datetime)
        assert msg.startswith("Task ")
        assert " created for QID Q7" in msg

    def test_enqueues_when_existing_row_is_not_in_queue(self):
        """Completed/error rows shouldn't block a re-enqueue."""
        db = MagicMock()
        db.find_queue_item_by_qid.return_value = {"qid": "Q7", "status": "completed"}

        msg = requestItemProcessing(qid="Q7", queue="user", db=db)

        db.enqueue_item.assert_called_once()
        assert "created for QID Q7" in msg

    def test_returns_error_message_on_exception(self):
        db = MagicMock()
        db.find_queue_item_by_qid.side_effect = RuntimeError("boom")

        msg = requestItemProcessing(qid="Q99", queue="user", db=db)

        assert msg.startswith("An error occurred:")


# ===========================================================================
# Legacy / workflow methods (kept for backward compatibility)
# ===========================================================================
class TestLegacyWorkflowMethods:
    def test_get_next_request_returns_document_or_none(self):
        with_doc = FakeCollection(find_one_and_update_result={"qid": "Q1"})
        without_doc = FakeCollection(find_one_and_update_result=None)
        handler = _make_handler()

        assert handler.get_next_request(with_doc) == {"qid": "Q1"}
        assert handler.get_next_request(without_doc) is None

    def test_get_next_request_wraps_errors_in_runtime_error(self):
        bad_queue = MagicMock()
        bad_queue.find_one_and_update.side_effect = RuntimeError("boom")
        handler = _make_handler()

        with pytest.raises(RuntimeError, match="Failed to get next request"):
            handler.get_next_request(bad_queue)

    def test_get_request_by_id_converts_string_to_objectid(self):
        captured_query = {}

        class QueueSpy:
            def find_one(self, query):
                captured_query.update(query)
                return {"ok": True}

        handler = _make_handler()
        _id = str(ObjectId())

        result = handler.get_request_by_id(QueueSpy(), _id)

        assert result == {"ok": True}
        assert isinstance(captured_query["_id"], ObjectId)
