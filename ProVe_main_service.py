from datetime import datetime
import time
from threading import Lock
from typing import List
import random
import signal
import sys

import nltk
from pymongo import collection
import schedule
from SPARQLWrapper import SPARQLWrapper, JSON
from torch.nn import Module
import yaml

from background_processing import (
    process_top_viewed_items,
    process_pagepile_list,
    process_random_qid
)
import ProVe_main_process
from utils.logger import logger
from utils.mongo_handler import MongoDBHandler

try:
    nltk.download('punkt_tab')
except Exception as e:
    logger.error(f"Error downloading nltk data: {e}")


class RandomItemCollector:
    def __init__(self):
        self.sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        
    def get_random_qids(self, num_qids, max_retries=10, delay=1):
        query = """
            SELECT ?item {
                SERVICE bd:sample {
                    ?item wikibase:sitelinks [].
                    bd:serviceParam bd:sample.limit "100".
                }
                MINUS {?item wdt:P31/wdt:P279* wd:Q4167836.}
                MINUS {?item wdt:P31/wdt:P279* wd:Q4167410.}
                MINUS {?item wdt:P31 wd:Q13406463.}
                MINUS {?item wdt:P31/wdt:P279* wd:Q11266439.}
                MINUS {?item wdt:P31 wd:Q17633526.}
                MINUS {?item wdt:P31 wd:Q13442814.}
                MINUS {?item wdt:P3083 [].}
                MINUS {?item wdt:P1566 [].}
                MINUS {?item wdt:P442 [].}
            }
        """
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        
        for attempt in range(max_retries):
            try:
                results = self.sparql.query().convert()
                all_qids = [result["item"]["value"].split("/")[-1] 
                           for result in results["results"]["bindings"]]
                return random.sample(all_qids, min(num_qids, len(all_qids)))
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.error(f"Error: {e}. {delay} seconds later retry...")
                    time.sleep(delay)
        return []

class ProVeService:
    def __init__(
        self,
        config_path,
        priority_queue: str,
        secondary_queue: List[str] = None
    ) -> None:
        self.config = self._load_config(config_path)
        self.running = True
        self.task_lock = Lock()
        self.setup_signal_handlers()
        self.models: List[Module] = None
        self.priority_queue = priority_queue
        self.secondary_queue = secondary_queue or []
        
        # Schedule settings
        schedule.every().day.at("02:00").do(self.run_top_viewed_items)
        schedule.every().saturday.at("03:00").do(self.run_pagepile_list)

    def _load_config(self, config_path):
        """Load configuration from yaml file"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except Exception as e:
            logger.error(f"Error loading config file: {e}")
            return {}
            
    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
    def handle_shutdown(self, signum, frame):
        logger.fatal("Shutdown signal received. Cleaning up...")
        self.running = False
        
    def initialize_resources(self):
        """Initialize models and database with retry mechanism"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print("Attempting to initialize resources...")
                logger.info("Attempting to initialize resources...")
                self.mongo_handler = MongoDBHandler()
                logger.info("WikiDataMongoDB connection successful")

                logger.info("Initializing queues...")
                self.priority_queue = getattr(self.mongo_handler, self.priority_queue, None)
                if self.priority_queue is None:
                    raise ValueError(f"Priority queue '{self.priority_queue}' not found in MongoDBHandler")
                secondary_queues = [getattr(self.mongo_handler, queue) for queue in self.secondary_queue]
                if len(secondary_queues) != len(self.secondary_queue):
                    raise ValueError(f"One or more secondary queues not found in MongoDBHandler: {self.secondary_queue}")
                self.secondary_queues = secondary_queues
                logger.info("Queues initialized successfully")
                
                print("Initializing models...")
                # self.models = ProVe_main_process.initialize_models()
                logger.info("Models initialized successfully")
                print("Resources initialized successfully")
                
                return True
            except Exception as e:
                print(f"Initialization attempt {attempt + 1} failed: {str(e)}")
                logger.error(f"Initialization attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error("Failed to initialize resources")
                    return False
                time.sleep(5)
                
    def main_loop(self, status_dict):
        """Process single request with lock mechanism"""
        with self.task_lock:
            try:
                self.mongo_handler.ensure_connection()
                self.mongo_handler.save_status(status_dict)

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
                status_dict['completed_timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                self.mongo_handler.save_status(status_dict)
                from functions import get_summary
                get_summary(qid, update=True)

            except Exception as e:
                logger.error(f"Error processing task {task_id}: {e}")
                status_dict['status'] = 'error'
                status_dict['error_message'] = str(e)
                self.mongo_handler.save_status(status_dict)

    def retry_processing(self, queue: collection) -> None:
        """Retry processing for items stuck in processing state."""
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
                logger.error(f"QID {item['qid']} has reached the maximum retry limit. Updating status to error.")
                # Update the status to error if retry limit is reached
                queue.update_one(
                    {'_id': item['_id']},
                    {'$set': {'status': 'error', 'error_message': 'Max retry limit reached'}}
                )

    def run(self):
        """Main service loop with improved error handling"""
        try:
            if not self.initialize_resources():
                logger.fatal("Failed to initialize resources. Exiting...")
                sys.exit(1)

            logger.info("Service started successfully")
            
            while self.running:
                try:
                    in_queue = self.mongo_handler.status_collection.find(
                        {'status': 'in queue'},
                        sort=[('requested_timestamp', 1)]
                    )
                    print("Items in queue:")
                    for item in in_queue:
                        print(item)
                    exit()
                    self.mongo_handler.ensure_connection()
                    status_dict = self.mongo_handler.get_next_request(self.priority_queue)
                    
                    if status_dict:
                        logger.info(f"Processing request for QID: {status_dict['qid']}")
                        self.main_loop(status_dict)
                    elif self.secondary_queues:
                        for queue in self.secondary_queues:
                            status_dict = self.mongo_handler.get_next_request(queue)
                            if status_dict:
                                logger.info(f"Processing request from secondary queue for QID: {status_dict['qid']}")
                                self.main_loop(status_dict)
                                break

                    self.retry_processing(self.priority_queue)
                    for queue in self.secondary_queues:
                        self.retry_processing(queue)
                    schedule.run_pending()
                    time.sleep(1)

                except Exception as e:
                    print(f"Main loop error: {str(e)}")
                    logger.error(f"Main loop error: {str(e)}")
                    time.sleep(30)
                
        except Exception as e:
            print(f"Fatal error in service: {str(e)}")
            logger.fatal(f"Fatal error in service: {str(e)}")
            sys.exit(1)
        print("Service stopped gracefully.")

    def run_top_viewed_items(self):
        logger.info("Running process_top_viewed_items...")
        process_top_viewed_items(limit=300)

    def run_pagepile_list(self):
        logger.info("Running process_pagepile_list...")
        process_pagepile_list()

    def check_and_run_random_qid(self):
        print(self.mongo_handler.status_collection.find_one({'status': 'in queue'}))
        if not self.mongo_handler.status_collection.find_one({'status': 'in queue'}):
            logger.info("No QIDs in queue, running process_random_qid...")
            for _ in range(5): 
                process_random_qid()
        exit()

if __name__ == "__main__":
    #MongoDBHandler().reset_database() #reset database
    service = ProVeService('config.yaml', 'status_collection', ['random_collection', 'user_collection'])
    print("Starting ProVe service...")
    service.run()

    # Process entity
    """ 
    qid = 'Q44'
    task_id = str(uuid.uuid4())
    status = 'processing'
    algo_version = '1.0'
    request_type = 'userRequested'
    requested_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    processing_start_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    completed_timestamp = 'null'
    
    status_dict = {
        'qid': qid,
        'task_id': task_id,
        'status': status,
        'algo_version': algo_version,
        'request_type': request_type,
        'requested_timestamp': requested_timestamp,
        'processing_start_timestamp': processing_start_timestamp,
        'completed_timestamp': completed_timestamp
    }
    
    
    """
