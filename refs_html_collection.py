import logging
from typing import Dict, Any, List
import yaml
import requests
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


class HTMLFetcher:
    HTTP_ERROR_MESSAGES = {
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        408: "Request Timeout",
        429: "Too Many Requests",
        500: "Internal Server Error",
        502: "Bad Gateway",
        503: "Service Unavailable",
        504: "Gateway Timeout"
    }

    def __init__(self, config_path: str = 'config.yaml'):
        """Initialize HTMLFetcher with configuration"""
        self.config = load_config(config_path)
        self.fetching_driver = self.config.get('html_fetching', {}).get('fetching_driver', 'requests')
        self.batch_size = self.config.get('html_fetching', {}).get('batch_size', 20)
        self.delay = self.config.get('html_fetching', {}).get('delay', 1.0)
        self.timeout = self.config.get('html_fetching', {}).get('timeout', 50)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def get_error_message(self, status_code: int) -> str:
        """Get descriptive error message for HTTP status code"""
        return self.HTTP_ERROR_MESSAGES.get(status_code, "Unknown Error")

    def fetch_html_with_requests(self, url: str) -> str:
        """Fetch HTML content using requests library"""
        try:
            response = requests.get(
                url, 
                timeout=self.timeout,
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            )
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            return f"Error: {str(e)}"

    def fetch_html_with_selenium(self, url: str) -> str:
        """Fetch HTML content using selenium"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        try:
            with webdriver.Chrome(options=chrome_options) as driver:
                driver.set_page_load_timeout(self.timeout)
                driver.get(url)
                time.sleep(1)  # Short delay to ensure page loads
                return driver.page_source
        except Exception as e:
            logging.error(f"Selenium error for {url}: {e}")
            return f"Error: {str(e)}"

    def fetch_all_html(self, url_df: pd.DataFrame, parser_result: Dict) -> pd.DataFrame:
        """
        Fetch HTML for all URLs in the DataFrame and add metadata from parser_result
        """
        result_df = url_df.copy()
        result_df['html'] = None
        result_df['status'] = None
        result_df['lang'] = None
        result_df['fetch_timestamp'] = None

        for i, (idx, row) in enumerate(result_df.iterrows()):
            if i > 0 and i % self.batch_size == 0:
                time.sleep(self.delay)

            try:
                fetch_start_time = pd.Timestamp.now()
                
                if self.fetching_driver == 'selenium':
                    html = self.fetch_html_with_selenium(row['url'])
                    status = 200 if not html.startswith('Error:') else 500
                else:
                    response = requests.get(
                        row['url'],
                        timeout=self.timeout,
                        headers=self.headers
                    )
                    status = response.status_code
                    if status == 200:
                        html = response.text
                    else:
                        error_msg = self.get_error_message(status)
                        html = f"Error: HTTP {status} - {error_msg}"

                result_df.at[idx, 'status'] = status
                result_df.at[idx, 'html'] = html
                result_df.at[idx, 'fetch_timestamp'] = fetch_start_time

                if status == 200:
                    try:
                        soup = BeautifulSoup(html, 'lxml')
                        lang = soup.html.get('lang', '')
                        if not lang:
                            meta_lang = soup.find('meta', attrs={'http-equiv': 'content-language'})
                            if meta_lang:
                                lang = meta_lang.get('content', '')
                        result_df.at[idx, 'lang'] = lang if lang else None
                    except Exception as e:
                        logging.error(f"Error detecting language for {row['url']}: {e}")
                        result_df.at[idx, 'lang'] = None

                logging.info(f"Successfully fetched HTML for {row['url']} (Status: {status}, Time: {fetch_start_time})")

            except requests.exceptions.HTTPError as e:
                status = e.response.status_code
                error_msg = self.get_error_message(status)
                result_df.at[idx, 'status'] = status
                result_df.at[idx, 'html'] = f"Error: HTTP {status} - {error_msg} - {str(e)}"
                result_df.at[idx, 'lang'] = None
                logging.error(f"HTTP error for {row['url']}: {status} ({error_msg})")
                result_df.at[idx, 'fetch_timestamp'] = pd.Timestamp.now()

            except requests.exceptions.Timeout as e:
                result_df.at[idx, 'status'] = 408
                result_df.at[idx, 'html'] = f"Error: HTTP 408 - Request Timeout - {str(e)}"
                result_df.at[idx, 'lang'] = None
                logging.error(f"Timeout error for {row['url']}: {e}")
                result_df.at[idx, 'fetch_timestamp'] = pd.Timestamp.now()

            except Exception as e:
                result_df.at[idx, 'status'] = 500
                result_df.at[idx, 'html'] = f"Error: HTTP 500 - Internal Server Error - {str(e)}"
                result_df.at[idx, 'lang'] = None
                logging.error(f"Failed to fetch HTML for {row['url']}: {e}")
                result_df.at[idx, 'fetch_timestamp'] = pd.Timestamp.now()

        # After fetching HTML, add metadata from parser_result
        claims_with_refs = parser_result['claims'].merge(
            parser_result['claims_refs'],
            on='claim_id',
            how='inner'
        )

        result_df = result_df.merge(
            claims_with_refs[['claim_id', 'entity_id', 'entity_label', 
                             'property_id', 'datavalue', 'reference_id']],
            on='reference_id',
            how='left'
        )

        # Fix "No label" entity labels
        if result_df['entity_label'].iloc[0].startswith('No label'):
            # Get the unique entity_id
            entity_id = result_df['entity_id'].iloc[0]  # Retrieve the unique entity_id

            # Fetch the label using SPARQL
            missing_labels = self.get_labels_from_sparql([entity_id])
            
            # Update the label if it exists
            if entity_id in missing_labels:
                result_df['entity_label'] = missing_labels[entity_id]  # Update the label

        # Extract object_id and object_label from datavalue
        def extract_object_id(datavalue: str) -> str:
            try:
                value_dict = eval(datavalue)
                if 'value' in value_dict and 'numeric-id' in value_dict['value']:
                    return f"Q{value_dict['value']['numeric-id']}"
            except:
                return None
            return None

        result_df['object_id'] = result_df['datavalue'].apply(extract_object_id)
        
        # Get property labels using the same method as in EvidenceSelector
        property_ids = result_df['property_id'].unique().tolist()
        object_ids = [oid for oid in result_df['object_id'].unique() if oid is not None]
        
        # Get labels from Wikidata (you'll need to implement this part)
        labels = self.get_labels_from_sparql(object_ids)
        
        # Add labels
        result_df['property_label'] = result_df['property_id'].map(labels)
        result_df['object_label'] = result_df['object_id'].map(labels)
        
        # Drop datavalue column as it's no longer needed
        result_df = result_df.drop('datavalue', axis=1)

        return result_df

    def get_labels_from_sparql(self, entity_ids: List[str]) -> Dict[str, str]:
        """
        Get labels for entities using SPARQL
        """
        endpoint_url = "https://query.wikidata.org/sparql"
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; MyBot/1.0; mailto:your@email.com)'
        }
        
        # Prepare the SPARQL query for a single entity
        query = f"""
        SELECT ?id ?label WHERE {{
          wd:{entity_ids[0]} rdfs:label ?label .
          BIND(wd:{entity_ids[0]} AS ?id)
          FILTER(LANG(?label) = "en" || LANG(?label) = "mul")
        }}
        """
        
        try:
            r = requests.get(endpoint_url, 
                            params={'format': 'json', 'query': query},
                            headers=headers)
            r.raise_for_status()
            results = r.json()
            
            # Create dictionary for labels
            labels = {}
            for result in results['results']['bindings']:
                label = result['label']['value']
                entity_id = result['id']['value'].split('/')[-1]
                labels[entity_id] = label
            
            return labels
            
        except Exception as e:
            logging.error(f"Error fetching labels: {e}")
            return {}

            
if __name__ == "__main__":
    qid = 'Q42'
    
    # Get URLs from WikidataParser
    from wikidata_parser import WikidataParser
    parser = WikidataParser()
    parser_result = parser.process_entity(qid)
    url_references_df = parser_result['urls']
    
    # Fetch HTML content with metadata
    fetcher = HTMLFetcher(config_path='config.yaml')
    result_df = fetcher.fetch_all_html(url_references_df, parser_result)
    
    print(f"Successfully processed {len(result_df)} URLs")
