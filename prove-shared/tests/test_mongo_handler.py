from datetime import datetime

from bson import ObjectId

from prove_shared.mongo_handler import MongoDBHandler, requestItemProcessing


class DummyQueue:
    def __init__(self, find_one_value=None, find_one_and_update_value=None, should_raise=False):
        self.find_one_value = find_one_value
        self.find_one_and_update_value = find_one_and_update_value
        self.should_raise = should_raise
        self.last_find_one_query = None
        self.last_find_one_and_update_args = None

    def find_one(self, query):
        self.last_find_one_query = query
        if self.should_raise:
            raise RuntimeError("find_one failed")
        return self.find_one_value

    def find_one_and_update(self, query, update, sort=None, return_document=None):
        self.last_find_one_and_update_args = {
            "query": query,
            "update": update,
            "sort": sort,
            "return_document": return_document,
        }
        if self.should_raise:
            raise RuntimeError("find_one_and_update failed")
        return self.find_one_and_update_value


def test_request_item_processing_returns_skip_for_existing_qid():
    queue = DummyQueue(find_one_value={"qid": "Q42", "status": "in queue"})

    msg = requestItemProcessing(
        qid="Q42",
        queue=queue,
        save_function=lambda _doc: None,
    )

    assert "already in queue" in msg


def test_request_item_processing_returns_created_message_and_calls_save():
    queue = DummyQueue(find_one_value=None)
    saved = {}

    def _save(doc):
        saved.update(doc)

    msg = requestItemProcessing(
        qid="Q7",
        queue=queue,
        request_type="userRequested",
        algo_version="1.2.3",
        save_function=_save,
    )

    assert msg.startswith("Task ")
    assert " created for QID Q7" in msg
    assert saved["qid"] == "Q7"
    assert saved["status"] == "in queue"
    assert saved["algo_version"] == "1.2.3"
    assert isinstance(saved["requested_timestamp"], datetime)


def test_request_item_processing_returns_error_message_on_exception():
    queue = DummyQueue(should_raise=True)

    msg = requestItemProcessing(
        qid="Q99",
        queue=queue,
        save_function=lambda _doc: None,
    )

    assert msg.startswith("An error occurred:")


def test_get_next_request_returns_document_or_none():
    handler = MongoDBHandler.__new__(MongoDBHandler)

    queue_with_doc = DummyQueue(find_one_and_update_value={"qid": "Q1"})
    queue_without_doc = DummyQueue(find_one_and_update_value=None)

    assert handler.get_next_request(queue_with_doc) == {"qid": "Q1"}
    assert handler.get_next_request(queue_without_doc) is None


def test_get_next_request_wraps_errors_in_runtime_error():
    handler = MongoDBHandler.__new__(MongoDBHandler)
    bad_queue = DummyQueue(should_raise=True)

    try:
        handler.get_next_request(bad_queue)
        assert False, "Expected RuntimeError"
    except RuntimeError as exc:
        assert "Failed to get next request" in str(exc)


def test_get_request_by_id_converts_string_to_objectid():
    handler = MongoDBHandler.__new__(MongoDBHandler)

    class QueueSpy:
        def __init__(self):
            self.query = None

        def find_one(self, query):
            self.query = query
            return {"ok": True}

    queue = QueueSpy()
    _id = str(ObjectId())

    result = handler.get_request_by_id(queue, _id)

    assert result == {"ok": True}
    assert isinstance(queue.query["_id"], ObjectId)
