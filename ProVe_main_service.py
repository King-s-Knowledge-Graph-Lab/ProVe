import wikidata_reader
import html_fetching
import reference_checking
import pandas as pd
import sqlite3
import os
from SPARQLWrapper import SPARQLWrapper, JSON
import random
import datetime
import time
import uuid
import yaml
import schedule
from pymongo import MongoClient
from datetime import datetime
import ProVe_main_process
from threading import Lock
import signal
import sys

class MongoDBHandler:
    def __init__(self, connection_string="mongodb://localhost:27017/", max_retries=3):
        self.connection_string = connection_string
        self.max_retries = max_retries
        self.connect()
    
    def connect(self):
        for attempt in range(self.max_retries):
            try:
                self.client = MongoClient(self.connection_string)
                self.client.server_info()  # Test connection
                self.db = self.client['wikidata_verification']
                self.html_collection = self.db['html_content']
                self.entailment_collection = self.db['entailment_results']
                self.stats_collection = self.db['parser_stats']
                self.status_collection = self.db['status']
                print("Successfully connected to MongoDB")
                return
            except Exception as e:
                print(f"MongoDB connection attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(5)  # Wait before retry

    def ensure_connection(self):
        """Ensure MongoDB connection is alive, reconnect if needed"""
        try:
            self.client.server_info()
        except:
            print("MongoDB connection lost, attempting to reconnect...")
            self.connect()

    def save_html_content(self, html_df):
        """Save HTML content data with task_id"""
        try:
            if html_df.empty:
                print("Warning: html_df is empty")
                return
                
            print(f"Attempting to save {len(html_df)} HTML records")
            records = html_df.to_dict('records')
            
            for record in records:
                try:
                    if 'reference_id' not in record:
                        print(f"Warning: record missing reference_id: {record}")
                        continue
                    
                    # Convert pandas Timestamp to datetime
                    if 'fetch_timestamp' in record and isinstance(record['fetch_timestamp'], pd.Timestamp):
                        record['fetch_timestamp'] = record['fetch_timestamp'].to_pydatetime()
                    
                    # Add save timestamp
                    record['save_timestamp'] = datetime.now()
                    
                    result = self.html_collection.update_one(
                        {
                            'reference_id': record['reference_id'],
                            'task_id': record['task_id']
                        },
                        {'$set': record},
                        upsert=True
                    )
                    
                    print(f"Updated HTML document with reference_id {record['reference_id']}: "
                          f"matched={result.matched_count}, modified={result.modified_count}, "
                          f"upserted_id={result.upserted_id}")
                          
                except Exception as e:
                    print(f"Error saving HTML record: {record}")
                    print(f"Error details: {e}")
                    
        except Exception as e:
            print(f"Error in save_html_content: {e}")
            raise

    def save_entailment_results(self, entailment_df):
        """Save entailment results with task_id"""
        try:
            if entailment_df.empty:
                print("Warning: entailment_df is empty")
                return
            
            print(f"Attempting to save {len(entailment_df)} entailment records")
            records = entailment_df.to_dict('records')
            
            for record in records:
                try:
                    # Convert timestamp string to datetime object
                    if 'processed_timestamp' in record:
                        record['processed_timestamp'] = datetime.strptime(
                            record['processed_timestamp'], 
                            '%Y-%m-%dT%H:%M:%S.%f'
                        )
                    
                    # Add save timestamp
                    record['save_timestamp'] = datetime.now()
                    
                    # Insert new document without checking for duplicates
                    result = self.entailment_collection.insert_one(record)
                    
                    print(f"Inserted new entailment document with reference_id {record['reference_id']}: "
                          f"inserted_id={result.inserted_id}")
                    
                except Exception as e:
                    print(f"Error saving entailment record: {record}")
                    print(f"Error details: {e}")
                    
        except Exception as e:
            print(f"Error in save_entailment_results: {e}")
            raise

    def save_parser_stats(self, stats_dict):
        """Save parser statistics with task_id"""
        try:
            # Convert Pandas Timestamp to datetime
            if isinstance(stats_dict.get('parsing_start_timestamp'), pd.Timestamp):
                stats_dict['parsing_start_timestamp'] = stats_dict['parsing_start_timestamp'].to_pydatetime()
            
            # Add save timestamp
            stats_dict['save_timestamp'] = datetime.now()
            
            result = self.stats_collection.update_one(
                {
                    'entity_id': stats_dict['entity_id'],
                    'task_id': stats_dict['task_id']
                },
                {'$set': stats_dict},
                upsert=True
            )
            
            print(f"Updated parser stats for entity {stats_dict['entity_id']}")
            
        except Exception as e:
            print(f"Error in save_parser_stats: {e}")
            raise

    def save_status(self, status_dict):
        """Save task status information to MongoDB"""
        try:
            # List of timestamp fields to process
            timestamp_fields = [
                'requested_timestamp',
                'processing_start_timestamp',
                'completed_timestamp'
            ]
            
            # Convert string timestamps to datetime objects
            for field in timestamp_fields:
                if status_dict.get(field) and status_dict[field] != 'null':
                    if isinstance(status_dict[field], str):
                        status_dict[field] = datetime.strptime(
                            status_dict[field].rstrip('Z'), 
                            '%Y-%m-%dT%H:%M:%S.%f'
                        )
            
            # Add last update timestamp
            status_dict['last_updated'] = datetime.now()
            
            # Find existing document by task_id and qid
            existing_doc = self.status_collection.find_one({
                'task_id': status_dict['task_id'],
                'qid': status_dict['qid']
            })
            
            if existing_doc:
                # Update existing document
                result = self.status_collection.update_one(
                    {
                        'task_id': status_dict['task_id'],
                        'qid': status_dict['qid']
                    },
                    {'$set': status_dict}
                )
                print(f"Updated status for task {status_dict['task_id']}: "
                      f"matched={result.matched_count}, modified={result.modified_count}")
            else:
                # Insert new document
                result = self.status_collection.insert_one(status_dict)
                print(f"Created new status for task {status_dict['task_id']}: "
                      f"inserted_id={result.inserted_id}")
            
        except Exception as e:
            print(f"Error in save_status: {e}")
            raise

    def reset_database(self):
        """Reset all collections in the database"""
        try:
            # Drop all collections
            self.html_collection.drop()
            self.entailment_collection.drop()
            self.stats_collection.drop()
            self.status_collection.drop()
            
            print("All collections have been reset successfully")
            
        except Exception as e:
            print(f"Error resetting database: {e}")
            raise

    def get_next_user_request(self):
        """
        Get the next pending user request from status collection
        Returns a dictionary with request information or None if no requests found
        """
        try:
            # Find the oldest user request that hasn't been processed
            pending_request = self.status_collection.find_one(
                {
                    'request_type': 'userRequested',
                    'status': 'in queue'
                },
                sort=[('requested_timestamp', 1)]  # Get oldest request first
            )
            
            if pending_request:
                # Update status to processing and add processing start timestamp
                status_dict = {
                    'qid': pending_request['qid'],
                    'task_id': pending_request['task_id'],
                    'status': 'processing',
                    'algo_version': pending_request.get('algo_version', '1.0'),
                    'request_type': pending_request['request_type'],
                    'requested_timestamp': pending_request['requested_timestamp'],
                    'processing_start_timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                    'completed_timestamp': 'null'
                }
                
                # Update the document in MongoDB
                self.save_status(status_dict)
                
                return status_dict
            
            return None
            
        except Exception as e:
            print(f"Error getting next user request: {e}")
            return None

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
                    print(f"Error: {e}. {delay} seconds later retry...")
                    time.sleep(delay)
        return []

class ProVeService:
    def __init__(self, config_path):
        self.config = self._load_config(config_path)
        self.running = True
        self.task_lock = Lock()
        self.setup_signal_handlers()
        
    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
    def handle_shutdown(self, signum, frame):
        print("\nShutdown signal received. Cleaning up...")
        self.running = False
        
    def initialize_resources(self):
        """Initialize models and database with retry mechanism"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.models = ProVe_main_process.initialize_models()
                self.mongo_handler = MongoDBHandler()
                return True
            except Exception as e:
                print(f"Initialization attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
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
                
            except Exception as e:
                print(f"Error processing task {task_id}: {e}")
                status_dict['status'] = 'error'
                status_dict['error_message'] = str(e)
                self.mongo_handler.save_status(status_dict)
        
    def run(self):
        """Main service loop with improved error handling"""
        if not self.initialize_resources():
            print("Failed to initialize resources. Exiting...")
            return

        while self.running:
            try:
                self.mongo_handler.ensure_connection()
                status_dict = self.mongo_handler.get_next_user_request()
                
                if status_dict:
                    print(f"Processing request for QID: {status_dict['qid']}")
                    self.main_loop(status_dict)
                else:
                    print("No pending user requests found. Waiting...")
                    time.sleep(10)
                    
            except Exception as e:
                print(f"Main loop error: {e}")
                time.sleep(30)



if __name__ == "__main__":

    #MongoDBHandler().reset_database() #reset database
    service = ProVeService('config.yaml')
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