from datetime import datetime, timedelta
import random
import time
import uuid

import pandas as pd
from pymongo import MongoClient
import requests
import yaml

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
                logger.info("Successfully connected to WikiData MongoDB")
                return
            except Exception as e:
                logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    logger.error("Failed to connect to MongoDB after multiple attempts")
                    raise
                time.sleep(5)  # Wait before retry

    def ensure_connection(self):
        """Ensure MongoDB connection is alive, reconnect if needed"""
        try:
            self.client.server_info()
        except:
            logger.info("MongoDB connection lost, attempting to reconnect...")
            self.connect()

    def save_html_content(self, html_df):
        """Save HTML content data with task_id"""
        try:
            if html_df.empty:
                logger.warning("html_df is empty")
                return

            logger.info(f"Attempting to save {len(html_df)} HTML records")
            records = html_df.to_dict('records')
            
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
                logger.info(
                    f"Updated status for task {status_dict['task_id']}: "
                    f"matched={result.matched_count}, modified={result.modified_count}"
                )
            else:
                # Insert new document
                result = self.status_collection.insert_one(status_dict)
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
            # Drop all collections
            self.html_collection.drop()
            self.entailment_collection.drop()
            self.stats_collection.drop()
            self.status_collection.drop()
            
            logger.info("All collections have been reset successfully")
            
        except Exception as e:
            logger.error(f"Error resetting database: {e}")
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
            logger.error(f"Error getting next user request: {e}")
            return None

# Load config
def load_config(config_path: str):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

config = load_config('config.yaml')
algo_version = config['version']['algo_version']
mongo_handler = MongoDBHandler()

def requestItemProcessing(qid: str, request_type: str) -> str:
    """
    Request processing for a specific QID
    
    Args:
        qid: Wikidata QID
        request_type: Type of processing request
        
    Returns:
        str: Status message
    """
    try:
        # Check if item is already in queue
        existing_request = mongo_handler.status_collection.find_one({
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
        
        # Insert into MongoDB
        mongo_handler.status_collection.insert_one(status_dict)
        return f"Successfully queued QID {qid} for processing"
        
    except Exception as e:
        logger.error(f"Error processing request for QID {qid}: {e}")
        return f"Error processing request for QID {qid}: {str(e)}"

def fetch_qid_by_label(label):
    """
    Fetch QID for a given label using SPARQL.
    """
    url = "https://query.wikidata.org/sparql"
    query = f"""
    SELECT ?item WHERE {{
      ?item rdfs:label "{label}"@en.
    }}
    """
    headers = {
        "User-Agent": "ProVe/1.1.0 (jongmo.kim@kcl.ac.uk)",
        "Accept": "application/sparql-results+json"
    }
    response = requests.get(url, params={"query": query}, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        results = data.get("results", {}).get("bindings", [])
        if results:
            return results[0]["item"]["value"].split("/")[-1]  # Extract QID from URI
        return None  # No QID found
    else:
        logger.error(f"Error fetching QID for label {label}: {response.text}")
        return None

def fetch_top_pageviews_and_qid(project, access, year, month, day, limit=10):
    """
    Fetches the top viewed pages and their QIDs.
    
    Args:
        project: The Wikipedia project (e.g., "en.wikipedia").
        access: The access method (e.g., "all-access").
        year: The year of the data.
        month: The month of the data.
        day: The day of the data.
        limit: The maximum number of articles to return.
        
    Returns:
        List of tuples containing (title, views, QID).
    """
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{project}/{access}/{year}/{month}/{day}"
    headers = {
        "User-Agent": "ProVe/1.1.0 (jongmo.kim@kcl.ac.uk)"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        articles = data.get('items', [])[0].get('articles', [])
        top_articles = []
        
        for article in articles:  # Iterate through all articles
            title = article['article'].replace("_", " ")  # Replace underscores with spaces
            views = article['views']
            qid = fetch_qid_by_label(title)  # Use the correct function to fetch QID
            
            # Exclude specific titles
            if title in ["Main Page", "Special:Search"]:
                logger.info(f"Excluding title: {title}")
                continue
            
            # Debugging output
            if qid is None:
                logger.info(f"QID not found for title: {title}")
            
            top_articles.append((title, views, qid))
            
            # Stop if we have reached the desired limit
            if len(top_articles) >= limit:
                break
        
        return top_articles
    else:
        logger.error(f"Error fetching top pageviews: {response.text}")
        return None

def process_top_viewed_items(project="en.wikipedia", access="all-access", limit=5):
    """
    Process the top viewed items from yesterday and queue them for processing.
    
    Args:
        project: The Wikipedia project (e.g., "en.wikipedia").
        access: The access method (e.g., "all-access").
        limit: The maximum number of articles to return.
    """
    # Get yesterday's date
    yesterday = datetime.utcnow() - timedelta(days=1)
    year = yesterday.strftime("%Y")
    month = yesterday.strftime("%m")
    day = yesterday.strftime("%d")

    # Fetch top viewed items
    top_items = fetch_top_pageviews_and_qid(project, access, year, month, day, limit)

    if top_items:
        logger.info("Top viewed items from yesterday:")
        for idx, (title, views, qid) in enumerate(top_items, 1):
            logger.info(f"{idx}. Title: {title} - {views} views (QID: {qid})")
            
            # Queue each item for processing
            if qid:  # Only queue if QID is found
                result = requestItemProcessing(qid, 'top_viewed')
                logger.info(f"   Queue status: {result}")
    else:
        logger.info("No articles found.")

def process_pagepile_list(file_path='utils/pagepileList.txt'):
    """
    Process the QIDs from the pagepile list file and queue them for processing.
    
    Args:
        file_path: The path to the pagepile list file.
    """
    try:
        with open(file_path, 'r') as file:
            qids = file.read().splitlines()
        
        for qid in qids:
            if qid:  # Ensure the QID is not empty
                result = requestItemProcessing(qid, 'pagepile_weekly_update')
                logger.info(f"Queued QID {qid} for processing: {result}")
    except Exception as e:
        logger.error(f"Error processing pagepile list: {e}")

def process_random_qid():
    """
    Generate a random QID and queue it for processing.
    """
    random_number = random.randint(0, 129999999)  # Generate a random number less than 130,000,000
    random_qid = f"Q{random_number}"  # Create QID by prefixing 'Q'
    
    # Queue the random QID for processing
    result = requestItemProcessing(random_qid, 'Random_processing')
    logger.info(f"Queued random QID {random_qid} for processing: {result}")

if __name__ == "__main__":
    # Uncomment the desired function to run
    # process_top_viewed_items(limit=300)  # Process top viewed items
    process_pagepile_list()  # Process QIDs from pagepile list
    #process_random_qid()  # Process a random QID
    