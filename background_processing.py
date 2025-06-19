from datetime import datetime, timedelta
import random

import pandas as pd
import requests
import yaml

from utils.logger import logger
from utils.mongo_handler import MongoDBHandler, requestItemProcessing


# Load config
def load_config(config_path: str):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


config = load_config('config.yaml')
algo_version = config['version']['algo_version']
mongo_handler = MongoDBHandler()


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


def process_system_qid(qid: str) -> None:
    """
    Queue system QID for processing.

    Args:
        qid: The QID to process.

    Raises:
        ValueError: If the QID does not start with 'Q'.
    """
    if not qid.startswith('Q'):
        try:
            int(qid)  # Check if the random QID is a valid integer
            qid = f"Q{qid}"
        except ValueError as e:
            raise ValueError("Generated QID does not start with 'Q'.") from e
    
    # Queue the random QID for processing
    result = requestItemProcessing(
        qid=qid,
        algo_version=algo_version,
        request_type='Random_processing',
        queue=mongo_handler.random_collection,
        save_function=mongo_handler.random_collection.insert_one
    )
    logger.info(f"Queued random QID {qid} for processing: {result}")


if __name__ == "__main__":
    # Uncomment the desired function to run
    # process_top_viewed_items(limit=300)  # Process top viewed items
    process_pagepile_list()  # Process QIDs from pagepile list
    #process_random_qid()  # Process a random QID
    