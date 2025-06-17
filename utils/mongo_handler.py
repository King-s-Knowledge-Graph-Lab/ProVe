from typing import Dict, Any, Callable
from datetime import datetime
import time
import uuid

import pandas as pd
from pymongo import MongoClient, collection

from utils.logger import logger

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
                self.summary_collection = self.db['summary']

                # Singular queues
                self.random_collection = self.db['random_queue']
                self.user_collection = self.db['user_queue']
 
                logger.info("Successfully connected to WikiData verification MongoDB")
                return
            except Exception as e:
                logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    logger.error("Failed to connect to MongoDB")
                    raise
                time.sleep(5)  # Wait before retry

    def ensure_connection(self):
        """Ensure MongoDB connection is alive, reconnect if needed"""
        try:
            self.client.server_info()
        except Exception as e:
            logger.error("MongoDB connection lost, attempting to reconnect...")
            logger.error(f"Error details: {e}")
            self.connect()

    def save_html_content(self, html_df):
        """Save HTML content data with task_id"""
        try:
            if html_df.empty:
                logger.warning("html_df is empty")
                return

            # Remvoing html data for storage efficiency 
            html_df_without_html = html_df.drop('html', axis=1)

            logger.info(f"Attempting to save {len(html_df_without_html)} HTML records")
            records = html_df_without_html.to_dict('records')
            
            for record in records:
                try:
                    if 'reference_id' not in record:
                        logger.warning(f"Record missing reference_id: {record}")
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

                    logger.info(
                        f"Updated HTML document with reference_id {record['reference_id']}: "
                        f"matched={result.matched_count}, modified={result.modified_count}, "
                        f"upserted_id={result.upserted_id}"
                    )
                          
                except Exception as e:
                    logger.error(f"Error saving HTML record: {record}")
                    logger.error(f"Error details: {e}")
                    
        except Exception as e:
            logger.error(f"Error in save_html_content: {e}")
            raise

    def save_entailment_results(self, entailment_df):
        """Save entailment results with task_id"""
        try:
            if entailment_df.empty:
                logger.warning("entailment_df is empty")
                return

            logger.info(f"Attempting to save {len(entailment_df)} entailment records")
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
                    logger.info(
                        f"Inserted new entailment document with reference_id {record['reference_id']}: "
                        f"inserted_id={result.inserted_id}"
                    )
                    
                except Exception as e:
                    logger.error(f"Error saving entailment record: {record}")
                    logger.error(f"Error details: {e}")
                    
        except Exception as e:
            logger.error(f"Error in save_entailment_results: {e}")
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

            logger.info(f"Updated parser stats for entity {stats_dict['entity_id']}")
            
        except Exception as e:
            logger.error(f"Error in save_parser_stats: {e}")
            raise

    def save_status(self, status_dict, queue: collection = None):
        """Save task status information to MongoDB"""
        if queue is None:
            queue = self.status_collection

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
            existing_doc = queue.find_one({
                'task_id': status_dict['task_id'],
                'qid': status_dict['qid']
            })

            if existing_doc:
                # Update existing document
                result = queue.update_one(
                    {
                        'task_id': status_dict['task_id'],
                        'qid': status_dict['qid']
                    },
                    {'$set': status_dict}
                )
                logger.info(
                    f"Updated status for task {status_dict['task_id']}: "
                    f"matched={result.matched_count}, modified={result.modified_count}"
                )
            else:
                # Insert new document
                result = queue.insert_one(status_dict)
                logger.info(
                    f"Created new status for task {status_dict['task_id']}: "
                    f"inserted_id={result.inserted_id}"
                )

        except Exception as e:
            logger.error(f"Error in save_status: {e}")
            raise

    def reset_database(self):
        """Reset all collections in the database"""
        try:
            self.html_collection.drop()
            self.entailment_collection.drop()
            self.stats_collection.drop()
            self.status_collection.drop()
            
            logger.info("All collections have been reset successfully")
            
        except Exception as e:
            logger.error(f"Error resetting database: {e}")
            raise

    def get_next_request(self, queue: collection):
        """
        Get the next pending request from a collection
        Returns a dictionary with request information or None if no requests found
        """
        try:
            # Find the oldest request that hasn't been processed
            pending_request = queue.find_one(
                {'status': 'in queue'},
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
                    'processing_start_timestamp': datetime.utcnow(),
                    'completed_timestamp': 'null'
                }
                
                # Update the document in MongoDB
                self.save_status(status_dict, queue)
                
                return status_dict
            return None  # No requests found
            
        except Exception as e:
            logger.error(f"Error getting next user request: {e}")
            return None


def requestItemProcessing(
    qid: str,
    queue: collection,
    request_type: str = 'userRequested',
    algo_version: str = '1.1.1',
    save_function: Callable[[Dict[str, Any]], None] = None
):
    """Request processing for a specific QID"""
    try:
        # Check if item is already in queue
        existing_request = queue.find_one({
            'qid': qid,
            'status': 'in queue'
        })
        
        if existing_request:
            return f"QID {qid} is already in queue. Skipping..."
        
        # Create new status document
        status_dict = {
            'qid': qid,
            'task_id': str(uuid.uuid4()),
            'status': 'in queue',
            'algo_version': algo_version,
            'request_type':  request_type,
            'requested_timestamp': datetime.utcnow(),
            'processing_start_timestamp': None,
            'completed_timestamp': None
        }

        # Save in respective queue
        save_function(status_dict)
        return f"Task {status_dict['task_id']} created for QID {qid}"
    except Exception as e:
        logger.error("Error in requestItemProcessing: %s", e)
        return f"An error occurred: {e}"