from datetime import datetime, timedelta

from bson import ObjectId

from prove_shared.objects import Entailment, HtmlContent, Status


def test_status_comparisons_and_formatted_timestamp_returns_expected_values():
    now = datetime.utcnow()
    later = now + timedelta(minutes=1)

    s1 = Status(
        _id=ObjectId(),
        qid="Q1",
        task_id="t1",
        status="in queue",
        algo_version="1.0",
        request_type="userRequested",
        requested_timestamp=now,
        processing_start_timestamp=now,
        completed_timestamp=now,
        last_updated=now,
    )
    s2 = Status(
        _id=ObjectId(),
        qid="Q1",
        task_id="t2",
        status="completed",
        algo_version="1.0",
        request_type="userRequested",
        requested_timestamp=later,
        processing_start_timestamp=later,
        completed_timestamp=later,
        last_updated=later,
    )

    assert s1 < s2
    assert s2 > s1
    assert s1 <= now
    assert s2 >= later
    assert s1 == now
    assert s1.get_formated_requested_timestamp().endswith("Z")


def test_htmlcontent_get_item_returns_error_payload_for_non_200_status():
    content = HtmlContent(
        reference_id="ref-1",
        task_id="task-1",
        entity_label="Entity",
        object_label="Object",
        property_label="prop",
        status=404,
        url="https://example.org",
        lang="en",
        object_id="Q123",
        property_id="P31",
    )

    item = content.get_item()

    assert item["qid"] == "Q123"
    assert item["property_id"] == "P31"
    assert item["result"] == "error"
    assert "HTTP Error code: 404" in item["result_sentence"]


def test_htmlcontent_add_info_item_updates_result_fields():
    content = HtmlContent(
        reference_id="ref-2",
        task_id="task-2",
        entity_label="Entity",
        object_label="Object",
        property_label="prop",
        status=200,
        url="https://example.org",
        lang="en",
        object_id="Q456",
        property_id="P279",
    )

    entailment = Entailment(
        _id=ObjectId(),
        text_entailment_score=0.9,
        similarity_score=0.8,
        processed_timestamp=datetime.utcnow(),
        result="SUPPORTS",
        result_sentence="Evidence sentence",
        reference_id="ref-2",
        label_probabilities={"SUPPORTS": 0.9, "REFUTES": 0.05, "NOT ENOUGH INFO": 0.05},
        task_id="task-2",
        save_timestamp=datetime.utcnow(),
    )

    content.add_info_item(entailment)
    item = content.get_item()

    assert item["result"] == "SUPPORTS"
    assert item["result_sentence"].endswith("/ Evidence sentence")
