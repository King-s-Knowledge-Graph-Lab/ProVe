from datetime import datetime
import time
from threading import Lock
from typing import List, Dict, Any, Union
import requests
import signal
import sys
import uuid

import nltk
from pymongo import collection
import schedule
from torch.nn import Module
import yaml

from background_processing import (
    process_top_viewed_items,
    process_pagepile_list,
)
import ProVe_main_process
from utils.logger import logger
from utils.mongo_handler import MongoDBHandler
from utils.local_secrets import ENDPOINT, API_KEY
from utils.auth import AsyncAuth


try:
    nltk.download('punkt_tab')
except Exception as e:
    logger.error(f"Error downloading nltk data: {e}")


class ProVeService:
    """    ProVeService is a service class that manages the ProVe processing pipeline.
    It initializes resources, sets up signal handlers for graceful shutdown, and runs the main
    processing loop. The service can process tasks from a priority queue and secondary queues
    in MongoDB. 

    Args:
        config_path (str): Path to the configuration file.
        priority_queue (str): Name of the priority queue in MongoDB.
        secondary_queue (List[str]): List of names of secondary queues in MongoDB

    Attributes:
        config (Dict[str, Any]): Configuration settings loaded from the YAML file.
        running (bool): A flag indicating whether the service is running.
        task_lock (Lock): A threading lock to ensure thread-safe operations.
        mongo_handler (MongoDBHandler): An instance of MongoDBHandler for database operations.
        models (List[Module]): A list of initialized models for processing tasks.
        priority_queue (collection): The priority queue collection in MongoDB.
        secondary_queue (List[collection]): A list of secondary queue collections in MongoDB.

    Raises:
        Exception: If there is an error loading the configuration file or initializing resources.
    """
    def __init__(
        self,
        config_path: str,
        priority_queue: str,
        secondary_queue: List[str] = []
    ) -> None:
        self.config = self._load_config(config_path)
        self.running = True
        self.task_lock = Lock()
        self.setup_signal_handlers()
        self.models: List[Module] = None
        self.priority_queue = priority_queue
        self.secondary_queue = secondary_queue or []
        self.uuid = uuid.uuid4()

        # Schedule settings
        schedule.every().day.at("02:00").do(self.run_top_viewed_items)
        schedule.every().saturday.at("03:00").do(self.run_pagepile_list)

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """ Load configuration from a YAML file.

        Args:
            config_path (str): Path to the configuration file.

        Returns:
            Dict[str, Any]:  A dictionary containing the configuration settings.

        Raises:
            Exception: If there is an error loading the configuration file.
        """
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except Exception as e:
            logger.error(f"Error loading config file: {e}")
            return {}

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, *args):
        """Handle shutdown signals gracefully."""
        logger.fatal("Shutdown signal received. Cleaning up...")
        self.running = False

    def get_public_key(self, count: int = 0) -> bool:
        response = requests.get(f"{ENDPOINT}getKey")
        if response.json().get("public key", None):
            public_key = response.json().get("public key").encode("utf-8")
            self.public_key = AsyncAuth.load_key(public_key, private=False)
            return True
        else:
            if count < 5:
                self.get_public_key(count + 1)
        return False

    def get_next_request(self, queue: str) -> Union[str, None]:
        api_key = AsyncAuth.encrypt(self.public_key, API_KEY)
        api_key = AsyncAuth.serialize(api_key)
        body = {
            "api_key": api_key,
            "uuid": str(self.uuid),
            "queue": "random" if "random" in queue else "user"
        }
        
        response = requests.post(
            f"{ENDPOINT}getNextQueue",
            json=body
        )
        logger.info(f"{queue} {response.json()}, {bool(response.json())}")
        if response.json():
            return response.json().get("_id")
        return None

    def initialize_resources(self, model: bool = True) -> bool:
        """
        Initialize resources for the ProVe service. This method initializes the MongoDB connection,
        sets up the priority and secondary queues, and optionally initializes the models if
        specified. It retries initialization up to three times with a delay between attempts in case
        of failure.

        Args:
            model (bool, optional): Loads models. Defaults to True.

        Raises:
            ValueError:  If any of the queues are not found in MongoDBHandler.
            ValueError:  If the models cannot be initialized.

        Returns:
            bool: True if initialization is successful, False otherwise.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info("Attempting to initialize resources...")
                self.mongo_handler = MongoDBHandler()
                logger.info("WikiDataMongoDB connection successful")

                logger.info("Initializing queues...")
                # Initialize priority queue
                priority_queue = getattr(self.mongo_handler, self.priority_queue, None)
                if priority_queue is None:
                    exception = f"Priority queue '{self.priority_queue}'"
                    exception += " not found in MongoDBHandler"
                    raise ValueError(exception)
                self.priority_queue = priority_queue

                # Initialize secondary queues
                secondary_queue = [
                    getattr(self.mongo_handler, queue) for queue in self.secondary_queue
                ]
                if len(secondary_queue) != len(self.secondary_queue):
                    exception = "One or more secondary queues not found in MongoDBHandler: "
                    exception += f"{self.secondary_queue}"
                    raise ValueError(exception)
                self.secondary_queue = secondary_queue
                logger.info("Queues initialized successfully")

                if model:
                    logger.info("Attempting to initialize models...")
                    self.models = ProVe_main_process.initialize_models()
                    logger.info("Models initialized successfully")
                
                return True
            except Exception as e:
                print(f"Initialization attempt {attempt + 1} failed: {str(e)}")
                logger.error(f"Initialization attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error("Failed to initialize resources")
                    return False
                time.sleep(5)

    def main_loop(self, status_dict: Dict[str, Any]) -> None:
        """
        Main processing loop for handling tasks.

        Args:
            status_dict (Dict[str, Any]): A dictionary containing the status of the task,
                including 'qid' and 'task_id'.
        """
        with self.task_lock:
            try:
                self.mongo_handler.ensure_connection()
                self.mongo_handler.save_status(status_dict)
                logger.info("Saved new status_dict into status")

                qid = status_dict['qid']
                task_id = status_dict['task_id']

                html_df, entailment_results, parser_stats = ProVe_main_process.process_entity(
                    qid, self.models
                )

                html_df['task_id'] = task_id
                entailment_results['task_id'] = task_id
                parser_stats['task_id'] = task_id

                self.mongo_handler.save_html_content(html_df)
                self.mongo_handler.save_entailment_results(entailment_results)
                self.mongo_handler.save_parser_stats(parser_stats)

                status_dict['status'] = 'completed'
                status_dict['completed_timestamp'] = datetime.utcnow().strftime(
                    '%Y-%m-%dT%H:%M:%S.%f'
                )
                self.mongo_handler.save_status(status_dict)
                logger.info("Updated new status_dict into status")
                try:
                    from functions import get_summary
                    get_summary(qid, update=True)
                except Exception:
                    logger.error(f"Could not update summary for {qid}.")
                    logger.error("Process will continue to keep application consistent")

            except Exception as e:
                logger.error(f"Error processing task {task_id}: {e}")
                status_dict['status'] = 'error'
                status_dict['error_message'] = str(e)
                self.mongo_handler.save_status(status_dict)

    def retry_processing(self, queue: collection) -> None:
        """
        Retry processing items in the queue that are stuck in 'processing' state.

        Args:
            queue (collection): The MongoDB collection representing the queue to check.
        """
        retry_limit = 3

        # Find items that are in processing state
        stuck_items = queue.find({
            'status': 'processing'
        })

        for item in stuck_items:
            # Check the number of retries
            if item.get('retry_count', 0) < retry_limit:
                logger.info(f"Retrying QID {item['qid']}...")
                # Increment the retry count
                queue.update_one(
                    {'_id': item['_id']},
                    {'$set': {'retry_count': item.get('retry_count', 0) + 1}}
                )
                # Reprocess the item
                self.main_loop(item)
            else:
                logger.error(f"QID {item['qid']} has reached the maximum retry limit.")
                # Update the status to error if retry limit is reached
                queue.update_one(
                    {'_id': item['_id']},
                    {'$set': {'status': 'error', 'error_message': 'Max retry limit reached'}}
                )

    def run(self):
        """
        Start the ProVe service. This method initializes the service, sets up the MongoDB
        connection, and starts the main processing loop. It runs indefinitely until a shutdown
        signal is received.

        Raises:
            SystemExit: If the service fails to initialize resources or encounters a fatal error.
        """
        try:
            if not self.initialize_resources():
                logger.fatal("Failed to initialize resources. Exiting...")
                sys.exit(1)

            if not self.get_public_key():
                logger.fatal("Failed to fetch public key from queue manager.")
                sys.exit(1)

            logger.info("Service started successfully")

            while self.running:
                try:
                    self.mongo_handler.ensure_connection()
                    _id = self.get_next_request(self.priority_queue.name)
                    logger.info(f"Next request {_id}")

                    status_dict = {}
                    if _id:
                        status_dict = self.mongo_handler.get_request_by_id(self.priority_queue, _id)

                    if status_dict:
                        logger.info(f"Processing request for QID: {status_dict['qid']}")
                        queue = self.priority_queue
                        self.main_loop(status_dict)
                        self.update_request(queue, status_dict, "completed")
                    elif self.secondary_queue:
                        for queue in self.secondary_queue:
                            _id = self.get_next_request(queue.name)
                            status_dict = {}
                            if _id:
                                status_dict = self.mongo_handler.get_request_by_id(queue, _id)
                            logger.info(f"{_id}: {status_dict}")

                            if status_dict:
                                message = "Processing request from secondary queue for QID: "
                                message += f"{status_dict['qid']}"
                                logger.info(message)
                                self.main_loop(status_dict)
                                self.update_request(queue, status_dict, "completed")
                                break

                    schedule.run_pending()
                except Exception as e:
                    logger.error(f"Main loop error: {str(e)}")
                    time.sleep(30)

        except Exception as e:
            logger.fatal(f"Fatal error in service: {str(e)}")
            sys.exit(1)

    def update_request(self, queue, status_dict, status):
        logger.info(f"Updating {status_dict['qid']} for {queue.name}")
        queue.update_one(
            {'task_id': status_dict['task_id'], 'qid': status_dict['qid']},
            {'$set': {'status': status}}
        )
        logger.info(f"Updated {status_dict['qid']} original request with {status}")

    def run_top_viewed_items(self):
        logger.info("Running process_top_viewed_items...")
        process_top_viewed_items(limit=300)

    def run_pagepile_list(self):
        logger.info("Running process_pagepile_list...")
        process_pagepile_list()


if __name__ == "__main__":
    service = ProVeService(
        config_path='config.yaml',
        priority_queue='user_collection',
        secondary_queue=['random_collection']
    )
    service.run()
