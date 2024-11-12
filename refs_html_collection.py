import logging
from typing import Dict, Any
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
        self.timeout = self.config.get('html_fetching', {}).get('timeout', 5)
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

    def fetch_all_html(self, url_df: pd.DataFrame) -> pd.DataFrame:
        """Fetch HTML for all URLs in the DataFrame"""
        result_df = url_df.copy()
        result_df['html'] = None
        result_df['status'] = None
        result_df['lang'] = None

        for i, (idx, row) in enumerate(result_df.iterrows()):
            if i > 0 and i % self.batch_size == 0:
                time.sleep(self.delay)

            try:
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

                logging.info(f"Successfully fetched HTML for {row['url']} (Status: {status})")

            except requests.exceptions.HTTPError as e:
                status = e.response.status_code
                error_msg = self.get_error_message(status)
                result_df.at[idx, 'status'] = status
                result_df.at[idx, 'html'] = f"Error: HTTP {status} - {error_msg} - {str(e)}"
                result_df.at[idx, 'lang'] = None
                logging.error(f"HTTP error for {row['url']}: {status} ({error_msg})")

            except requests.exceptions.Timeout as e:
                result_df.at[idx, 'status'] = 408
                result_df.at[idx, 'html'] = f"Error: HTTP 408 - Request Timeout - {str(e)}"
                result_df.at[idx, 'lang'] = None
                logging.error(f"Timeout error for {row['url']}: {e}")

            except Exception as e:
                result_df.at[idx, 'status'] = 500
                result_df.at[idx, 'html'] = f"Error: HTTP 500 - Internal Server Error - {str(e)}"
                result_df.at[idx, 'lang'] = None
                logging.error(f"Failed to fetch HTML for {row['url']}: {e}")

        return result_df

            
if __name__ == "__main__":
    qid = 'Q44'
    
    # Get URLs from WikidataParser
    parser = WikidataParser()
    parser_result = parser.process_entity(qid)
    url_references_df = parser_result['urls']
    
    # Fetch HTML content
    fetcher = HTMLFetcher(config_path='config.yaml')
    result_df = fetcher.fetch_all_html(url_references_df)
    
    print(f"Successfully processed {len(result_df)} URLs")
