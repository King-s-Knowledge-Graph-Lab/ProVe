from collections import defaultdict
from threading import BoundedSemaphore
from typing import Any, Dict, Union
import sys

from pymongo import collection

from local_secrets import CODE_PATH, MAX_CONNECTIONS
from utils_api import logger

sys.path.append(CODE_PATH)
from utils.mongo_handler import MongoDBHandler


class QueueManager:
    def __init__(self, queue: str):
        self.mongodb = MongoDBHandler()
        self.queue: collection = getattr(self.mongodb, queue, None)
        if self.queue is None:
            logger.error(f"MongoDB has no queue with name {queue}")
            raise ValueError(f"MongoDB has no queue with name {queue}")

        self.semaphore = BoundedSemaphore(MAX_CONNECTIONS)
        self.request_tracker = defaultdict(str)
        self.ensure_consistency_in_queue()

    def reset_request(self, request_identifier: str) -> bool:
        if request_identifier is None:
            logger.error("Identifier can't be None")
            raise ValueError("Identifier can't be None")

        if request_identifier not in self.request_tracker.keys():
            return True

        with self.semaphore:
            document = self.mongodb.get_request_by_id_and_reset(
                self.queue,
                self.request_tracker[request_identifier],
            )

        if document:
            logger.info(f"Set document '{self.request_tracker[request_identifier]}' back to queue")
            return True

        logger.error(f"Couldn't find document '{self.request_tracker[request_identifier]}'")
        return False

    def ensure_consistency_per_service(self, request_identifier: str) -> bool:
        if request_identifier is None:
            logger.error("Identifier can't be None")
            raise ValueError("Identifier can't be None")

        if request_identifier not in self.request_tracker.keys():
            return True

        _id = self.request_tracker[request_identifier]
        document = self.mongodb.get_request_by_id(self.queue, _id)

        if document:
            if document["status"] != "completed":
                logger.warning(f"Error in {self.queue.name} for document {_id}, returning to queue")
                self.mongodb.get_request_by_id_and_reset(self.queue, _id)
                return False

            status_confirmation = self.mongodb.get_request_by_taskid(
                self.mongodb.status_collection,
                document["task_id"],
            )

            if status_confirmation:
                if status_confirmation["status"] != "completed":
                    logger.warning(f"Error for document {_id} in status, returning to queue")
                    self.mongodb.get_request_by_id_and_reset(self.queue, _id)
                    return False
                logger.info(f"Document {_id} consistent, nothig to be done")
                return True
            else:
                logger.warning(f"Document {_id} not in status, returning to queue")
                self.mongodb.get_request_by_id_and_reset(self.queue, _id)
                return False
        else:
            logger.error("Could not find document")
            raise ValueError(f"Could not find document {_id} in {self.queue.name}")

    def ensure_consistency_in_queue(self) -> None:
        documents = self.mongodb.get_all_request_in_progress(self.queue)
        for document in documents:
            _id = document["_id"]
            if _id in self.request_tracker.values():
                logger.info(f"Document {_id} already tracked, consistency confirmed")
                continue

            status_confirmation = self.mongodb.get_request_by_taskid(
                self.mongodb.status_collection,
                document["task_id"],
            )

            if status_confirmation:
                if status_confirmation["status"] == "completed":
                    logger.warning(f"Document {_id} 'completed' in status, updating queue")
                    self.mongodb.set_request_status_and_processing_time(
                        self.queue,
                        status_confirmation["status"],
                        status_confirmation["processing_start_timestamp"],
                        _id,
                    )
                else:
                    logger.warning(f"Error for document {_id} status, returning to queue")
                    self.mongodb.get_request_by_id_and_reset(self.queue, _id)
            else:
                logger.warning(f"Document {_id} not in status, returning to queue")
                self.mongodb.get_request_by_id_and_reset(self.queue, _id)

    def get_next_in_queue(
        self,
        request_identifier: str,
        retry_count: int = 0
    ) -> Union[Dict[str, Any], None]:
        if request_identifier is None:
            logger.error("Identifier can't be None")
            raise ValueError("Identifier can't be None")

        try:
            self.mongodb.ensure_connection()
        except ConnectionError:
            logger.error("No MongoDB conncection")
            if retry_count >= 5:
                return None
            return self.get_next_in_queue(retry_count + 1)

        if request_identifier in self.request_tracker:
            logger.info(f"Confirming {request_identifier} last job")

        with self.semaphore:
            status_dict = self.mongodb.get_next_request(self.queue)

        if status_dict:
            _id = status_dict.get('_id')
            self.request_tracker[request_identifier] = _id
            return {'_id': str(_id)}
        return None

    def confirm_processing(self, request_identifier: str, retry_count: int = 0) -> bool:
        if request_identifier is None:
            logger.error("Identifier can't be None")
            raise ValueError("Identifier can't be None")

        try:
            self.mongodb.ensure_connection()
        except ConnectionError:
            logger.error("No MongoDB conncection")
            if retry_count >= 5:
                return False
            return self.confirm_processing(retry_count + 1)

        status = self.ensure_consistency_per_service(request_identifier)
        if status:
            self.request_tracker[request_identifier] = None
        return status
