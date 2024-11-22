from ProVe_main_service import MongoDBHandler
from datetime import datetime
import yaml
import uuid
import requests
import pdb

mongo_handler = MongoDBHandler()

#Params.
def load_config(config_path: str):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)
config = load_config('config.yaml')
algo_version = config['version']['algo_version']

def requestItemProcessing(qid, request_type):
    """Request processing for a specific QID"""
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
            'requested_timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            'processing_start_timestamp': None,
            'completed_timestamp': None
        }
        
        # Save to MongoDB
        result = mongo_handler.save_status(status_dict)
        
        return f"Task {status_dict['task_id']} created for QID {qid}"
        
    except Exception as e:
        return f"An error occurred: {e}"

def fetch_top_viewed_items(project="en.wikipedia", year="2024", month="11", day="01"):
    """
    Fetch the top viewed items from Wikimedia for a given project and date.

    Args:
        project (str): Wikimedia project to fetch data from (e.g., 'en.wikipedia', 'ko.wikipedia').
        year (str): Year of the data (e.g., '2024').
        month (str): Month of the data (e.g., '11').
        day (str): Day of the data (e.g., '01').

    Returns:
        None: Prints the top viewed items to the console.
    """
    # Construct the API URL
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{project}/all-access/{year}/{month}/{day}"
    
    try:
        # Make a GET request to the Wikimedia API
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Parse the response JSON
        data = response.json()
        top_items = data.get("items", [])[0].get("articles", [])
        
        # Print the top 10 viewed items
        print(f"Top viewed items on {project} for {year}-{month}-{day}:")
        for idx, item in enumerate(top_items[:10], start=1):  # Limit to the top 10 items
            print(f"{idx}. {item['article']} - {item['views']} views")
    
    except requests.exceptions.RequestException as e:
        # Handle HTTP request errors
        print(f"Error fetching data: {e}")
    except (KeyError, IndexError):
        # Handle JSON parsing errors
        print("Unexpected response format.")
        
def daily_top_viewed_items():
    pass

def fetch_top_viewed_yesterday_with_qname(project="en.wikipedia", top_n=10):
    """
    Fetches top viewed items and processes them if they have valid QNames
    """
    try:
        # ... 기존 API 호출 코드 ...
        
        if articles:
            processed_count = 0
            for article in articles:
                title = article["article"].replace("_", " ")
                views = article["views"]
                
                # Filter out special pages
                if title.startswith("Special:") or title.startswith("Wikipedia:"):
                    continue
                
                qname = fetch_wikidata_qname(title, project)
                if qname != "No QName found" and qname != "Error fetching QName":
                    # Process the item
                    result = requestItemProcessing(qname, "top_viewed")
                    processed_count += 1
                    print(f"Processed {title} ({qname}) - {views} views: {result}")
                
                if processed_count >= top_n:
                    break
            
            return f"Processed {processed_count} items with valid QNames"
        else:
            return "No data available for yesterday."
    
    except Exception as e:
        return f"Error processing data: {e}"

def process_top_viewed_items(project="en.wikipedia", top_n=10):
    """
    Fetches top viewed items and automatically processes them if they have valid QNames.
    
    Args:
        project (str): Wikimedia project (e.g., 'en.wikipedia', 'ko.wikipedia')
        top_n (int): Maximum number of items to process
        
    Returns:
        str: Summary of processing results
    """
    try:
        # Get the top viewed items first
        top_items = fetch_top_viewed_yesterday_with_qname(project, top_n)
        if isinstance(top_items, str) and "Error" in top_items:
            return top_items
            
        # Process each line
        processed_count = 0
        for line in top_items.split('\n'):
            # Parse the line (format: "1. Title - 123 views - Q12345")
            parts = line.split(' - ')
            if len(parts) != 3:
                continue
                
            qname = parts[2].strip()
            if qname != "No QName found" and qname != "Error fetching QName":
                result = requestItemProcessing(qname, "top_viewed")
                processed_count += 1
                print(f"Processed {parts[0]} ({qname}): {result}")
        
        return f"Successfully processed {processed_count} items from top viewed list"
        
    except Exception as e:
        return f"Error in processing top viewed items: {e}"

if __name__ == "__main__":
    print("Fetching top viewed items:")
    print(fetch_top_viewed_yesterday_with_qname(project="en.wikipedia", top_n=10))
    
    print("\nProcessing top viewed items:")
    print(process_top_viewed_items(project="en.wikipedia", top_n=10))
    
