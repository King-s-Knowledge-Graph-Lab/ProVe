from typing import Dict, Any, Callable, Union
from datetime import datetime
from bson import ObjectId
import time
import uuid

import pandas as pd
from pymongo import MongoClient, collection, database, ReturnDocument

from utils.logger import logger


class MongoDBHandler:
    """
    MongoDBHandler is a class that manages the connection to a MongoDB database and provides methods
    to save HTML content, entailment results, parser statistics, and other data related to WikiData
    verification tasks. It handles connection retries, ensures the connection is alive, and
    provides methods to save various types of data with appropriate error handling and logging.

    Args:
        connection_string (str): The MongoDB connection string. Defaults to "mongodb://localhost:27017/".
        max_retries (int): Maximum number of retries for connecting to MongoDB. Defaults to 3.

    Attributes:
        max_retries (int): Maximum number of retries for connecting to MongoDB.
        connection_string (str): The MongoDB connection string.
        client (MongoClient): The MongoDB client instance.
        db (database): The database instance.
        html_collection (collection): Collection for storing HTML content.
        entailment_collection (collection): Collection for storing entailment results.
        stats_collection (collection): Collection for storing parser statistics.
        status_collection (collection): Collection for storing task status.
        summary_collection (collection): Collection for storing task summaries.
        random_collection (collection): Singular queue for random tasks.
        user_collection (collection): Singular queue for user tasks.

    Raises:
        ConnectionError: If the connection to MongoDB fails after the maximum number of retries.
    """
    def __init__(
        self,
        connection_string: str = "mongodb://localhost:27017/",
        max_retries: int = 3,
    ) -> None:
        # MongoDB connection parameters
        self.max_retries = max_retries
        self.connection_string = connection_string

        # Initialize MongoDB client and collections attributes
        self.client: MongoClient = None
        self.db: database = None
        self.html_collection: collection = None
        self.entailment_collection: collection = None
        self.stats_collection: collection = None
        self.status_collection: collection = None
        self.summary_collection: collection = None
        self.random_collection: collection = None
        self.user_collection: collection = None

        # Attempt to connect to MongoDB
        if not self.connect(max_retries, connection_string):
            logger.error("Failed to connect to MongoDB")
            raise ConnectionError("Could not connect to MongoDB after multiple attempts")

    def connect(self, max_retries: int, connection_string: str) -> bool:
        """
        Connect to MongoDB with retries.

        Args:
            max_retries (int): Maximum number of retries for connecting to MongoDB.
            connection_string (str): The MongoDB connection string.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        for attempt in range(max_retries):
            try:
                self.client = MongoClient(connection_string)
                self.ensure_connection(try_reconnect=False)
                break
            except Exception as e:
                logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    return False
                time.sleep(5)
                continue

        # Access the database and collections
        self.db = self.client['wikidata_verification']
        self.html_collection = self.db['html_content']
        self.entailment_collection = self.db['entailment_results']
        self.stats_collection = self.db['parser_stats']
        self.status_collection = self.db['status']
        self.summary_collection = self.db['summary']

        # Singular queues
        self.random_collection = self.db['random_queue']
        self.user_collection = self.db['user_queue']

        # Set indexes for high concurrency
        try:
            self.random_collection.create_index([('status', 1), ('requested_timestamp', 1)])
        except Exception as e:
            logger.error(f"Failed to create index {e}")

        logger.info("Successfully connected to WikiData verification MongoDB")
        return True

    def ensure_connection(self, try_reconnect: bool = True) -> None:
        """
        Ensure MongoDB connection is alive, reconnect if needed

        Args:
            try_reconnect (bool): Whether to attempt reconnection if the connection is lost.
                Defaults to True.
        
        Raises:
            ConnectionError: If the connection cannot be re-established.
        """
        try:
            self.client.server_info()
        except Exception as e:
            logger.error("MongoDB connection lost, attempting to reconnect...")
            logger.error(f"Error details: {e}")

            if not try_reconnect:
                logger.error("Reconnection failed, please check MongoDB server status")
                raise ConnectionError("MongoDB connection lost") from e

            if not self.connect(self.max_retries, self.connection_string):
                logger.error("Reconnection failed, please check MongoDB server status")
                raise ConnectionError("Could not reconnect to MongoDB") from e

    def save_html_content(self, html_df: pd.DataFrame) -> None:
        """
        Save HTML content data with task_id.
        
        Args:
            html_df (pd.DataFrame): DataFrame containing HTML content with columns:
                - reference_id: Unique identifier for the HTML content.
                - task_id: Identifier for the task associated with the HTML content.
                - html: The actual HTML content as a string.
                - fetch_timestamp: Timestamp when the HTML was fetched.
            
        Raises:
            RuntimeError: If there is an error while saving HTML content to MongoDB.
        """
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
            raise RuntimeError(f"Failed to save HTML content: {e}") from e

    def save_entailment_results(self, entailment_df: pd.DataFrame) -> None:
        """
        Save entailment results to MongoDB.
        Args:
            entailment_df (pd.DataFrame): DataFrame containing entailment results with columns:
                - reference_id: Unique identifier for the entailment result.
                - task_id: Identifier for the task associated with the entailment result.
                - processed_timestamp: Timestamp when the entailment was processed.
        
        Raises:
            RuntimeError: If there is an error while saving entailment results to MongoDB.
        """
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
            raise RuntimeError(f"Failed to save entailment results: {e}") from e

    def save_parser_stats(self, stats_dict: Dict[str, Any]) -> None:
        """
        Save parser statistics to MongoDB.

        Args:
            stats_dict (Dict[str, Any]): Dictionary containing parser statistics with keys:
                - entity_id: Unique identifier for the entity.
                - task_id: Identifier for the task associated with the entity.
                - parsing_start_timestamp: Timestamp when parsing started.
                - save_timestamp: Timestamp when the stats were saved.
        
        Raises:
            RuntimeError: If there is an error while saving parser statistics to MongoDB.
        """
        try:
            # Convert Pandas Timestamp to datetime
            if isinstance(stats_dict.get('parsing_start_timestamp'), pd.Timestamp):
                stats_dict['parsing_start_timestamp'] = stats_dict[
                    'parsing_start_timestamp'
                ].to_pydatetime()
            
            # Add save timestamp
            stats_dict['save_timestamp'] = datetime.now()
            
            self.stats_collection.update_one(
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
            raise RuntimeError(f"Failed to save parser statistics: {e}") from e

    def save_status(self, status_dict: Dict[str, Any], queue: collection = None) -> None:
        """
        Save or update the status of a task in the specified queue.

        Args:
            status_dict (Dict[str, Any]): Dictionary containing status information with keys:
                - task_id: Unique identifier for the task.
                - qid: Unique identifier for the item being processed.
                - status: Current status of the task (e.g., 'in queue', 'processing', 'completed').
                - algo_version: Version of the algorithm used.
                - request_type: Type of request (e.g., 'userRequested').
                - requested_timestamp: Timestamp when the request was made.
                - processing_start_timestamp: Timestamp when processing started.
                - completed_timestamp: Timestamp when processing completed.
            queue (collection, optional): which MongoDB collection to save the status in.
                Defaults to status.
        """
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
            raise RuntimeError(f"Failed to save status: {e}") from e

    def get_next_request(self, queue: collection) -> Union[Dict[str, Any], None]:
        """
        Get the next user request from the queue.

        Args:
            queue (collection): The MongoDB collection to search for requests.

        Returns:
            Union[Dict[str, Any], None]: Entry of the next request to be processed,
                or None if no requests are found.

        Raises:
            RuntimeError: If there is an error while retrieving the next request.
        """
        try:
            pending_request = queue.find_one_and_update(
                {
                    'status': 'in queue',
                    'processing_start_timestamp': None
                },
                {'$set': {
                    'status': 'processing',
                    'processing_start_timestamp': datetime.utcnow(),
                }},
                sort=[('requested_timestamp', 1)],
                return_document=ReturnDocument.AFTER
            )

            if pending_request:
                return pending_request
            return None
        except Exception as e:
            logger.error(f"Error getting next user request: {e}")
            raise RuntimeError(f"Failed to get next request: {e}") from e

    def get_request_by_id_and_reset(
        self,
        queue: collection,
        _id: str
    ) -> Union[Dict[str, Any], None]:
        return queue.find_one_and_update(
            {
                '_id': _id,
                'status': 'processing',
                'processing_start_timestamp': {'$not': {'$eq': None}}
            },
            {'$set': {
                'status': 'in queue',
                'processing_start_timestamp': None
            }},
            return_document=ReturnDocument.AFTER
        )

    def set_request_status_and_processing_time(
        self,
        queue: collection,
        status: str,
        processing_time: datetime,
        _id: str
    ) -> Union[Dict[str, Any], None]:
        return queue.find_one_and_update(
            {'_id': _id},
            {'$set': {
                'status': status,
                'processing_start_timestamp': processing_time
            }},
            return_document=ReturnDocument.AFTER
        )

    def get_request_by_id(self, queue: collection, _id: str) -> Union[Dict[str, Any], None]:
        return queue.find_one({'_id': ObjectId(_id)})

    def get_request_by_taskid(self, queue: collection, task_id: str) -> Union[Dict[str, Any], None]:
        return queue.find_one({'task_id': task_id})

    def get_all_request_in_progress(self, queue: collection) -> Union[Dict[str, Any], None]:
        return queue.find({'status': 'processing'})


def requestItemProcessing(
    qid: str,
    queue: collection,
    request_type: str = 'userRequested',
    algo_version: str = '1.1.1',
    save_function: Callable[[Dict[str, Any]], None] = None
) -> str:
    """
    Request item processing by creating a new status document in the specified queue.

    Args:
        qid (str): Unique Wikidata identifier for the item being processed.
        queue (collection): The MongoDB collection where the status will be saved.
        request_type (str, optional): Whether the request is user requested or random.
            Defaults to 'userRequested'.
        algo_version (str, optional): Version of the algorithm used for processing.
            Defaults to '1.1.1'.
        save_function (Callable[[Dict[str, Any]], None], optional): Function to save the status
            document. This should be changed in next releases.

    Returns:
        result (str): A message indicating the result of the request processing.
    """
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
            'request_type': request_type,
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
