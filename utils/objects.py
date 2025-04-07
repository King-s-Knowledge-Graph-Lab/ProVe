from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any

from bson import ObjectId


@dataclass
class Status:

    _id: ObjectId
    qid: str
    task_id: str
    status: str
    algo_version: str
    request_type: str
    requested_timestamp: datetime
    processing_start_timestamp: datetime
    completed_timestamp: datetime
    last_updated: datetime
    retry_count: Optional[int] = field(default=0)
    error_message: Optional[str] = field(default=None)

    def __eq__(self, other: 'Status') -> bool:
        if isinstance(other, Status):
            return self.last_updated == other.last_updated
        elif isinstance(other, datetime):
            return self.last_updated == other
        else:
            return NotImplemented

    def __lt__(self, other: 'Status') -> bool:
        if isinstance(other, Status):
            return self.last_updated < other.last_updated
        elif isinstance(other, datetime):
            return self.last_updated < other
        else:
            return NotImplemented

    def __le__(self, other: 'Status') -> bool:
        if isinstance(other, Status):
            return self.last_updated <= other.last_updated
        elif isinstance(other, datetime):
            return self.last_updated <= other
        return NotImplemented

    def __gt__(self, other: 'Status') -> bool:
        if isinstance(other, Status):
            return self.last_updated > other.last_updated
        elif isinstance(other, datetime):
            return self.last_updated > other
        return NotImplemented

    def __ge__(self, other: 'Status') -> bool:
        if isinstance(other, Status):
            return self.last_updated >= other.last_updated
        elif isinstance(other, datetime):
            return self.last_updated >= other
        return NotImplemented

    def get_formated_requested_timestamp(self) -> str:
        if isinstance(self.requested_timestamp, datetime):
            return self.requested_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        return self.requested_timestamp


@dataclass
class Entailment:

    _id: ObjectId
    text_entailment_score: float
    similarity_score: float
    processed_timestamp: datetime
    result: str
    result_sentence: str
    reference_id: str
    label_probabilities: Dict[str, float]
    task_id: str
    save_timestamp: datetime


@dataclass
class HtmlContent:

    reference_id: str
    task_id: str
    entity_label: str
    object_label: str
    property_label: str
    status: int
    url: str
    _id: ObjectId = field(default=None)
    claim_id: str = field(default=None)
    entity_id: str = field(default=None)
    fetch_timestamp: datetime = field(default=None)
    lang: str = field(default=None)
    object_id: str = field(default=None)
    property_id: str = field(default=None)
    reference_datatype: str = field(default=None)
    reference_property_id: str = field(default=None)
    save_timestamp: datetime = field(default=None)
    item: Dict[str, Any] = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.item = {
            "qid": self.object_id,
            "property_id": self.property_id,
            "url": self.url,
            "triple": f"{self.entity_label} {self.property_label} {self.object_label}"
        }

        if self.status != 200:
            self.item["result"] = 'error'
            self.item["result_sentence"] = f"Source language: ({self.lang}) "
            self.item["result_sentence"] += f"/ HTTP Error code: {self.status}"

    def get_item(self) -> Dict[str, Any]:
        return self.item

    def add_info_item(self, entailment: Entailment) -> None:
        self.item["result"] = entailment.result
        self.item["result_sentence"] = f"Source language: ({self.lang}) "
        self.item["result_sentence"] += f"/ {entailment.result_sentence}"
