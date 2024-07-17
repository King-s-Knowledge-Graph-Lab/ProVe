import sqlite3
import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any, Tuple, Union
import yaml, requests, time
from requests.exceptions import RequestException
import utils.wikidata_utils as wdutils
from tqdm import tqdm
import ast, json
from datetime import datetime
import re, string, fasttext, pysbd, spacy, os, lxml, time
from bs4 import BeautifulSoup
from spacy.language import Language
from json.decoder import JSONDecodeError
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
import pdb

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


class WikidataObjectProcessor:
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = load_config(config_path)
        self.Wd_API = wdutils.CachedWikidataAPI()
        self.Wd_API.languages = ['en']
        self.dt_types = ['wikibase-item', 'monolingualtext', 'quantity', 'time', 'string']
        self.reset = self.config.get('parsing', {}).get('reset_database')

    @staticmethod
    def turn_to_century_or_millennium(y: int, mode: str) -> str:
        y = str(y)
        if mode == 'C':
            div, group, mode_name = 100, int(y.rjust(3, '0')[:-2]), 'century'
        elif mode == 'M':
            div, group, mode_name = 1000, int(y.rjust(4, '0')[:-3]), 'millennium'
        else:
            raise ValueError('Use mode = C for century and M for millennium')
        
        if int(y) % div != 0:
            group += 1
        group = str(group)
        
        group_suffix = 'th' if group[-1] in ['0', '4', '5', '6', '7', '8', '9'] else \
                       'st' if group[-1] == '1' else \
                       'nd' if group[-1] == '2' else 'rd'
        
        return f"{group}{group_suffix} {mode_name}"

    def process_monolingualtext(self, dv: str) -> Tuple[str, str]:
        dv_dict = ast.literal_eval(dv)
        return dv_dict['value']['text'], dv_dict['value']['language']

    def process_quantity(self, dv: str, attr: str) -> Tuple[str, str]:
        dv_dict = ast.literal_eval(dv)
        amount, unit = dv_dict['value']['amount'], dv_dict['value']['unit']
        amount = amount[1:] if amount[0] == '+' else amount
        
        if str(unit) == '1':
            return (str(amount), 'en') if attr == 'label' else ('no-desc', 'none')
        
        unit_entity_id = unit.split('/')[-1]
        if attr == 'label':
            unit_label = self.Wd_API.get_label(unit_entity_id, True)
            return f"{amount} {unit_label[0]}", unit_label[1]
        return getattr(self.Wd_API, f'get_{attr}')(unit_entity_id)

    def process_time(self, dv: str, attr: str) -> Tuple[str, str]:
        dv_dict = ast.literal_eval(dv)
        time, precision = dv_dict['value']['time'], dv_dict['value']['precision']
        assert dv_dict['value']['after'] == 0 and dv_dict['value']['before'] == 0

        suffix = 'BC' if time[0] == '-' else ''
        time = time[1:]

        try:
            parsed_time = datetime.strptime(time, '%Y-00-00T00:00:%SZ')
        except ValueError:
            parsed_time = datetime.strptime(time, '%Y-%m-%dT00:00:%SZ')

        if attr in ['desc', 'alias']:
            return ('no-desc', 'none') if attr == 'desc' else self.get_time_aliases(parsed_time, precision, suffix)

        if precision == 11:  # date
            return parsed_time.strftime('%d/%m/%Y') + suffix, 'en'
        elif precision == 10:  # month
            return parsed_time.strftime("%B of %Y") + suffix, 'en'
        elif precision == 9:  # year
            return parsed_time.strftime('%Y') + suffix, 'en'
        elif precision == 8:  # decade
            return parsed_time.strftime('%Y')[:-1] + '0s' + suffix, 'en'
        elif precision == 7:  # century
            return self.turn_to_century_or_millennium(parsed_time.year, 'C') + suffix, 'en'
        elif precision == 6:  # millennium
            return self.turn_to_century_or_millennium(parsed_time.year, 'M') + suffix, 'en'
        elif precision in [4, 3, 0]:  # hundred thousand, million, billion years
            timeint = int(parsed_time.strftime('%Y'))
            scale = {4: 1e5, 3: 1e6, 0: 1e9}[precision]
            unit = {4: 'hundred thousand', 3: 'million', 0: 'billion'}[precision]
            return f"{round(timeint/scale, 1)} {unit} years {suffix}", 'en'

    def get_time_aliases(self, parsed_time: datetime, precision: int, suffix: str) -> Tuple[List[str], str]:
        if precision == 11:  # date
            return ([
                parsed_time.strftime('%-d of %B, %Y') + suffix,
                parsed_time.strftime('%d/%m/%Y (dd/mm/yyyy)') + suffix,
                parsed_time.strftime('%b %-d, %Y') + suffix
            ], 'en')
        return ('no-alias', 'none')

    def process_string(self, dv: str) -> Tuple[str, str]:
        return ast.literal_eval(dv)['value'], 'en'



class HTMLFetcher:
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = load_config(config_path)
        self.db_name = self.config.get('database', {}).get('name', 'default_database.db')
        self.reset = self.config.get('parsing', {}).get('reset_database', False)
        self.conn = None
        self.cursor = None
        self.Wd_API = wdutils.CachedWikidataAPI()
        self.Wd_API.languages = ['en']
        self.BAD_DATATYPES = ['external-id', 'commonsMedia', 'url', 'globe-coordinate', 'wikibase-lexeme', 'wikibase-property']
        self.dt_types = ['wikibase-item', 'monolingualtext', 'quantity', 'time', 'string']
        self.fetching_driver = self.config.get('html_fetching', {}).get('fetching_driver', 'requests')

    def ensure_tables(self):
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_html (
                    url TEXT PRIMARY KEY,
                    html TEXT
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS claim_text (
                    reference_id TEXT,
                    entity_id TEXT,
                    claim_id TEXT,
                    rank TEXT,
                    property_id TEXT,
                    datatype TEXT,
                    datavalue TEXT,
                    entity_label TEXT,
                    entity_label_lan TEXT,
                    entity_alias TEXT,
                    entity_alias_lan TEXT,
                    entity_desc TEXT,
                    entity_desc_lan TEXT,
                    property_label TEXT,
                    property_label_lan TEXT,
                    property_alias TEXT,
                    property_alias_lan TEXT,
                    property_desc TEXT,
                    property_desc_lan TEXT,
                    object_label TEXT,
                    object_alias TEXT,
                    object_desc TEXT,
                    object_label_lan TEXT,
                    object_alias_lan TEXT,
                    object_desc_lan TEXT,
                    PRIMARY KEY (reference_id, entity_id, claim_id)
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS html_text (
                    entity_id TEXT,
                    reference_id TEXT,
                    reference_property_id TEXT,
                    reference_datatype TEXT,
                    url TEXT,
                    html TEXT,
                    extracted_sentences TEXT,
                    extracted_text TEXT,
                    nlp_sentences TEXT,
                    nlp_sentences_slide_2 TEXT,
                    PRIMARY KEY (entity_id, reference_id, url)
                )
            ''')
            self.conn.commit()
            logging.info("Ensured all tables exist")
        except sqlite3.Error as e:
            logging.error(f"An error occurred while ensuring tables: {e}")

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.ensure_tables()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def reset_tables(self):
        try:
            self.cursor.execute("DROP TABLE IF EXISTS url_html")
            self.cursor.execute("DROP TABLE IF EXISTS claim_text")
            self.cursor.execute("DROP TABLE IF EXISTS html_text")
            self.conn.commit()
            self.ensure_tables()
            logging.info("All tables have been reset")
        except sqlite3.Error as e:
            logging.error(f"An error occurred while resetting tables: {e}")

    def get_url_references(self, qids: List[str]) -> pd.DataFrame:
        qids_str = ', '.join(f"'{qid}'" for qid in qids)
        query = f"SELECT * FROM url_references WHERE entity_id IN ({qids_str})"
        
        try:
            df = pd.read_sql_query(query, self.conn)
            logging.info(f"Retrieved {len(df)} url references for {len(qids)} entities")
            return df
        except sqlite3.Error as e:
            logging.error(f"An error occurred while fetching url references: {e}")
            return pd.DataFrame()
    
    def create_url_html_table(self, url_references_df: pd.DataFrame):
        url_html_df = url_references_df[['url']].copy()
        url_html_df['html'] = None 
        try:
            for _, row in url_html_df.iterrows():
                self.cursor.execute('''
                    INSERT OR REPLACE INTO url_html (url, html)
                    VALUES (?, ?)
                ''', (row['url'], row['html']))
            self.conn.commit()
            logging.info(f"Updated url_html table with {len(url_html_df)} rows")
        except sqlite3.Error as e:
            logging.error(f"An error occurred while updating url_html table: {e}")
            self.conn.rollback()

    def reading_html_by_requests(self, url: str) -> None:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                html_content = response.text
            else:
                html_content = f"Error: HTTP status code {response.status_code}"
            
            self.cursor.execute('''
                UPDATE url_html
                SET html = ?
                WHERE url = ?
            ''', (html_content, url))
            logging.info(f"Updated HTML for URL: {url}")
        
        except RequestException as e:
            error_message = f"Error: {str(e)}"
            self.cursor.execute('''
                UPDATE url_html
                SET html = ?
                WHERE url = ?
            ''', (error_message, url))
            logging.error(f"Failed to fetch HTML for URL {url}: {error_message}")

    def reading_html_by_chrome(self, driver, url: str) -> None:
        try:
            driver.get(url)
            html_content = driver.page_source

            self.cursor.execute('''
                UPDATE url_html
                SET html = ?
                WHERE url = ?
            ''', (html_content, url))
            logging.info(f"Updated HTML for URL: {url}")

        except WebDriverException as e:
            error_message = f"Error: {str(e)}"
            self.cursor.execute('''
                UPDATE url_html
                SET html = ?
                WHERE url = ?
            ''', (error_message, url))
            logging.error(f"Failed to fetch HTML for URL {url}: {error_message}")


    def fetch_and_update_html(self):
        batch_size = self.config.get('html_fetching', {}).get('batch_size', 20)
        delay = self.config.get('html_fetching', {}).get('delay', 1.0)

        try:
            self.cursor.execute("SELECT url FROM url_html WHERE html IS NULL")
            urls_to_fetch = self.cursor.fetchall()
            
            if self.fetching_driver == 'requests':
                for i, (url,) in enumerate(urls_to_fetch):
                    if i > 0 and i % batch_size == 0:
                        self.conn.commit()
                        time.sleep(delay)  # Delay to avoid overwhelming the server

                    self.reading_html_by_requests(url)
            else:
                chrome_options = Options()
                chrome_options.add_argument("--headless")  
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                chrome_options.add_argument("--disable-plugins")
                chrome_options.add_argument("--disable-pdf-viewer")
                service = Service('/usr/bin/chromedriver')
                driver = webdriver.Chrome(service=service, options=chrome_options)
                chrome_options.add_experimental_option("prefs", {
                    "download.default_directory": "/CodeArchive/payloads/",
                    "download.prompt_for_download": False,
                    "download.directory_upgrade": True,
                    "plugins.always_open_pdf_externally": False,
                    "safebrowsing.enabled": True
                })
                driver.set_page_load_timeout(10) 
                for i, (url,) in enumerate(urls_to_fetch):
                    if i > 0 and i % batch_size == 0:
                        self.conn.commit()
                        time.sleep(delay)  # Delay to avoid overwhelming the server

                    self.reading_html_by_chrome(driver, url)
                if 'driver' in locals():
                    driver.quit()
                    
            self.conn.commit()
            logging.info(f"Completed updating HTML for {len(urls_to_fetch)} URLs")
        
        except sqlite3.Error as e:
            logging.error(f"An error occurred while updating HTML content: {e}")
            self.conn.rollback()
        
    def reference_id_to_claim_id(self, reference_id: str) -> np.ndarray:
        self.cursor.execute(f'SELECT claim_id FROM claims_refs WHERE reference_id=?', (reference_id,))
        sql_result = self.cursor.fetchall()
        return np.array(sql_result).reshape(-1)

    def reference_id_to_claim_data(self, reference_id: str) -> List[Tuple]:
        claim_ids = self.reference_id_to_claim_id(reference_id)
        results = []
        for claim_id in claim_ids:
            self.cursor.execute('SELECT * FROM claims WHERE claim_id=?', (claim_id,))
            results.extend(self.cursor.fetchall())
        return results

    def process_claim_data(self, text_reference_sampled_df_html: pd.DataFrame) -> pd.DataFrame:
        claims_columns = ['entity_id', 'claim_id', 'rank', 'property_id', 'datatype', 'datavalue']
        claim_data = []
        for reference_id in text_reference_sampled_df_html.reference_id:
            data = self.reference_id_to_claim_data(reference_id)
            claim_data.extend([(reference_id,) + t for t in data])

        claim_df = pd.DataFrame(claim_data, columns=['reference_id'] + claims_columns)
        claim_df = claim_df[~claim_df.datatype.isin(self.BAD_DATATYPES)].reset_index(drop=True)
        return claim_df
    
    def process_wikibase_item(self, dv: str, attr: str) -> Union[Tuple[str, str], List[str]]:
        item_id = ast.literal_eval(dv)['value']['id']
        if attr == 'label':
            return getattr(self.Wd_API, f'get_{attr}')(item_id, True)
        return getattr(self.Wd_API, f'get_{attr}')(item_id)
    
    def process_time(self, dv: str, attr: str) -> Tuple[str, str]:
        dv_dict = ast.literal_eval(dv)
        time, precision = dv_dict['value']['time'], dv_dict['value']['precision']
        assert dv_dict['value']['after'] == 0 and dv_dict['value']['before'] == 0

        suffix = 'BC' if time[0] == '-' else ''
        time = time[1:]

        try:
            parsed_time = datetime.strptime(time, '%Y-00-00T00:00:%SZ')
        except ValueError:
            parsed_time = datetime.strptime(time, '%Y-%m-%dT00:00:%SZ')

        if attr in ['desc', 'alias']:
            return ('no-desc', 'none') if attr == 'desc' else self.get_time_aliases(parsed_time, precision, suffix)

        if precision == 11:  # date
            return parsed_time.strftime('%d/%m/%Y') + suffix, 'en'
        elif precision == 10:  # month
            return parsed_time.strftime("%B of %Y") + suffix, 'en'
        elif precision == 9:  # year
            return parsed_time.strftime('%Y') + suffix, 'en'
        elif precision == 8:  # decade
            return parsed_time.strftime('%Y')[:-1] + '0s' + suffix, 'en'
        elif precision == 7:  # century
            return self.turn_to_century_or_millennium(parsed_time.year, 'C') + suffix, 'en'
        elif precision == 6:  # millennium
            return self.turn_to_century_or_millennium(parsed_time.year, 'M') + suffix, 'en'
        elif precision in [4, 3, 0]:  # hundred thousand, million, billion years
            timeint = int(parsed_time.strftime('%Y'))
            scale = {4: 1e5, 3: 1e6, 0: 1e9}[precision]
            unit = {4: 'hundred thousand', 3: 'million', 0: 'billion'}[precision]

            return f"{round(timeint/scale, 1)} {unit} years {suffix}", 'en'
        
    def process_monolingualtext(self, dv: str) -> Tuple[str, str]:
        dv_dict = ast.literal_eval(dv)
        return dv_dict['value']['text'], dv_dict['value']['language']

    def process_quantity(self, dv: str, attr: str) -> Tuple[str, str]:
        dv_dict = ast.literal_eval(dv)
        amount, unit = dv_dict['value']['amount'], dv_dict['value']['unit']
        amount = amount[1:] if amount[0] == '+' else amount
        
        if str(unit) == '1':
            return (str(amount), 'en') if attr == 'label' else ('no-desc', 'none')
        
        unit_entity_id = unit.split('/')[-1]
        if attr == 'label':
            unit_label = self.Wd_API.get_label(unit_entity_id, True)
            return f"{amount} {unit_label[0]}", unit_label[1]
        return getattr(self.Wd_API, f'get_{attr}')(unit_entity_id)
    def process_string(self, dv: str) -> Tuple[str, str]:
        return ast.literal_eval(dv)['value'], 'en'
    def get_time_aliases(self, parsed_time: datetime, precision: int, suffix: str) -> Tuple[List[str], str]:
        if precision == 11:  # date
            return ([
                parsed_time.strftime('%-d of %B, %Y') + suffix,
                parsed_time.strftime('%d/%m/%Y (dd/mm/yyyy)') + suffix,
                parsed_time.strftime('%b %-d, %Y') + suffix
            ], 'en')
        return ('no-alias', 'none')
    
    def add_labels_and_descriptions(self, claim_df: pd.DataFrame) -> pd.DataFrame:
        def query_for_basic(target_id):
            sparql_query = f"""
                SELECT ?label ?labelLang ?alias ?aliasLang ?description ?descLang
                WHERE {{
                OPTIONAL {{ 
                    {target_id} rdfs:label ?label.
                    FILTER(LANG(?label) IN ("en", "de", "fr", "es", "zh"))
                    BIND(LANG(?label) AS ?labelLang)
                }}
                OPTIONAL {{ 
                    {target_id} skos:altLabel ?alias. 
                    FILTER(LANG(?alias) IN ("en", "de", "fr", "es", "zh"))
                    BIND(LANG(?alias) AS ?aliasLang)
                }}
                OPTIONAL {{ 
                    {target_id} schema:description ?description. 
                    FILTER(LANG(?description) IN ("en", "de", "fr", "es", "zh"))
                    BIND(LANG(?description) AS ?descLang)
                }}
                }}
                ORDER BY 
                (IF(?labelLang = "en", 0, 1)) 
                ?labelLang 
                (IF(?aliasLang = "en", 0, 1)) 
                ?aliasLang 
                (IF(?descLang = "en", 0, 1)) 
                ?descLang
                Limit 3
            """
            try:
                df = self.Wd_API.custom_sparql_query(sparql_query).json()
                results = df.get('results', {}).get('bindings', [])
                if results:
                    first_result = results[0]
                    label = first_result.get('label', {}).get('value', 'No label')
                    alias = first_result.get('alias', {}).get('value', 'No alias')
                    desc = first_result.get('description', {}).get('value', 'No description')
                else:
                    label, alias, desc = 'No label', 'No alias', 'No description'
            except (JSONDecodeError, AttributeError, KeyError):
                label, alias, desc = 'No label', 'No alias', 'No description'

            return {'target_id': target_id, 'label': label, 'alias': alias, 'desc': desc}
        # for 'entity', finding label, alias, desc
        entity_basic = query_for_basic(f"wd:{claim_df['entity_id'][0]}")
        claim_df['entity_label'] = entity_basic['label']
        claim_df['entity_label_lan'] = 'en'
        claim_df['entity_alias'] = entity_basic['alias']
        claim_df['entity_alias_lan'] = 'en'
        claim_df['entity_desc'] = entity_basic['desc']
        claim_df['entity_desc_lan'] = 'en'
        # for 'property', finding label, alias, desc
        property_basic_li = []
        for prt in claim_df['property_id'].unique():

            property_basic_li.append(query_for_basic(f"wd:{prt}"))

        property_df = pd.DataFrame(property_basic_li)
        property_df['property_id'] = property_df['target_id'].str.replace('wd:', '')
        property_df['label_lan'] = 'en'
        property_df['alias_lan'] = 'en'
        property_df['desc_lan'] = 'en'

        result_df = claim_df.merge(property_df[['property_id', 'label', 'alias', 'desc', 'label_lan', 'alias_lan', 'desc_lan']],
                           on='property_id',
                           how='left')

        result_df = result_df.rename(columns={
            'label': 'property_label', 
            'alias': 'property_alias', 
            'desc': 'property_desc',
            'label_lan': 'property_label_lan',
            'alias_lan': 'property_alias_lan',
            'desc_lan': 'property_desc_lan'
        })
        claim_df = result_df

        def get_object_attribute(row: dict, attr: str) -> Union[Tuple[str, str], List[str]]:
            dt, dv = row['datatype'], row['datavalue']
            
            if dt not in self.dt_types:
                raise ValueError(f"Unexpected datatype: {dt}")
            
            try:
                if dt == 'wikibase-item':
                    return self.process_wikibase_item(dv, attr)
                elif dt == 'monolingualtext':
                    return self.process_monolingualtext(dv) if attr == 'label' else ('no-desc', 'none')
                elif dt == 'quantity':
                    return self.process_quantity(dv, attr)
                elif dt == 'time':
                    return self.process_time(dv, attr)
                elif dt == 'string':
                    return self.process_string(dv) if attr == 'label' else ('no-desc', 'none')
            except Exception as e:
                raise ValueError(f"Error processing {attr} for {dt}: {str(e)}")


        def get_object_label_given_datatype(row: dict) -> Tuple[str, str]:
            return get_object_attribute(row, 'label')

        def get_object_desc_given_datatype(row: dict) -> Tuple[str, str]:
            return get_object_attribute(row, 'desc')

        def get_object_alias_given_datatype(row: dict) -> Union[Tuple[str, str], List[str]]:
            return get_object_attribute(row, 'alias')
        claim_df['object_label'] = claim_df.apply(get_object_label_given_datatype, axis=1)
        claim_df['object_alias'] = claim_df.apply(get_object_alias_given_datatype, axis=1)
        claim_df['object_desc'] = claim_df.apply(get_object_desc_given_datatype, axis=1)

        for attr in ['label', 'alias', 'desc']:
            claim_df[f'object_{attr}'], claim_df[f'object_{attr}_lan'] = zip(*claim_df[f'object_{attr}'].apply(lambda x: x if isinstance(x, tuple) else (x, '')))
        return claim_df[claim_df['object_label_lan'] != 'none'].reset_index(drop=True)

    def process_qid(self, qid: str) -> pd.DataFrame:
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        try:
            # Step 1: Get claim_ids for the given qid
            cursor.execute("SELECT claim_id FROM claims WHERE entity_id = ?", (qid,))
            claim_ids = [row[0] for row in cursor.fetchall()]

            if not claim_ids:
                print(f"No claims found for QID: {qid}")
                return pd.DataFrame()

            # Step 2: Get reference_ids for the found claim_ids
            placeholders = ','.join('?' * len(claim_ids))
            cursor.execute(f"SELECT DISTINCT reference_id FROM claims_refs WHERE claim_id IN ({placeholders})", claim_ids)
            reference_ids = [row[0] for row in cursor.fetchall()]

            if not reference_ids:
                print(f"No references found for claims of QID: {qid}")
                return pd.DataFrame()

            # Step 3: Get url references for the found reference_ids
            placeholders = ','.join('?' * len(reference_ids))
            query = f"SELECT * FROM url_references WHERE reference_id IN ({placeholders})"
            html_set = pd.read_sql_query(query, conn, params=reference_ids)

            if html_set.empty:
                print(f"No URL references found for references of QID: {qid}")
                return pd.DataFrame()

            # Step 4: Get HTML content for the URLs
            urls = html_set['url'].tolist()
            placeholders = ','.join('?' * len(urls))
            query = f"SELECT url, html FROM url_html WHERE url IN ({placeholders})"
            html_content = pd.read_sql_query(query, conn, params=urls)

            # Step 5: Merge HTML content with html_set
            result = pd.merge(html_set, html_content, on='url', how='left')

            print(f"Processed QID: {qid}")
            print(f"Number of rows in result: {len(result)}")

            return result

        finally:
            conn.close()

    def claim2text(self, html_set):       
        claim_df = self.process_claim_data(html_set)
        print(f"Number of unique reference IDs: {claim_df.reference_id.nunique()}")
        return self.add_labels_and_descriptions(claim_df)
    
    def store_data_to_db(self, claim_text: pd.DataFrame, html_text: pd.DataFrame):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        for col in claim_text.columns:
            claim_text[col] = claim_text[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='claim_text'")
        if cursor.fetchone() is None:
            create_claim_table_query = '''
            CREATE TABLE claim_text (
                reference_id TEXT,
                entity_id TEXT,
                claim_id TEXT,
                rank TEXT,
                property_id TEXT,
                datatype TEXT,
                datavalue TEXT,
                entity_label TEXT,
                entity_label_lan TEXT,
                entity_alias TEXT,
                entity_alias_lan TEXT,
                entity_desc TEXT,
                entity_desc_lan TEXT,
                property_label TEXT,
                property_alias TEXT,
                property_desc TEXT,
                property_label_lan TEXT,
                property_alias_lan TEXT,
                property_desc_lan TEXT,
                object_label TEXT,
                object_alias TEXT,
                object_desc TEXT,
                object_label_lan TEXT,
                object_alias_lan TEXT,
                object_desc_lan TEXT,
                PRIMARY KEY (reference_id, entity_id, claim_id)
            )
            '''
            cursor.execute(create_claim_table_query)
            print("Created new 'claim_text' table.")
        else:
            print("'claim_text' table already exists. Appending data.")

        claim_text.to_sql('temp_claim_text', conn, if_exists='replace', index=False)
        
        cursor.execute('''
        INSERT OR REPLACE INTO claim_text
        SELECT * FROM temp_claim_text
        ''')
        
        cursor.execute('DROP TABLE temp_claim_text')
        list_columns = ['extracted_sentences', 'nlp_sentences', 'nlp_sentences_slide_2']
        for col in list_columns:
            html_text[col] = html_text[col].apply(json.dumps)
        for col in html_text.columns:
            if col not in list_columns:
                html_text[col] = html_text[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='html_text'")
        if cursor.fetchone() is None:
            create_html_table_query = '''
            CREATE TABLE html_text (
                entity_id TEXT,
                reference_id TEXT,
                reference_property_id TEXT,
                reference_datatype TEXT,
                url TEXT,
                html TEXT,
                extracted_sentences TEXT,
                extracted_text TEXT,
                nlp_sentences TEXT,
                nlp_sentences_slide_2 TEXT,
                PRIMARY KEY (entity_id, reference_id, url)
            )
            '''
            cursor.execute(create_html_table_query)
            print("Created new 'html_text' table.")
        else:
            print("'html_text' table already exists. Appending data.")

        html_text.to_sql('temp_html_text', conn, if_exists='replace', index=False)
        
        cursor.execute('''
        INSERT OR REPLACE INTO html_text
        SELECT * FROM temp_html_text
        ''')
        
        cursor.execute('DROP TABLE temp_html_text')

        conn.commit()
        conn.close()
        print(f"Data has been successfully stored in the database: {self.db_name}")
    
class HTMLTextProcessor:
    def __init__(self, config_path='config.yaml'):
        self.config = load_config(config_path)
        self.reset = self.config.get('parsing', {}).get('reset_database', False)
        self._RE_COMBINE_WHITESPACE = re.compile(r"\s+")
        self.ft_model = fasttext.load_model('base/lid.176.ftz')
        self.splitter = pysbd.Segmenter(language="en", clean=False)
        if not spacy.util.is_package("en_core_web_lg"):
            os.system("python -m spacy download en_core_web_lg")
        self.nlp = spacy.load("en_core_web_lg")


    def predict_language(self, text: str, k: int = 20) -> List[Tuple[str, float]]:
        ls, scores = self.ft_model.predict(text, k=k)
        ls = [l.replace('__label__', '') for l in ls]
        return list(zip(ls, scores))

    def get_url_language(self, html: str) -> Tuple[str, float]:
        try:
            soup = BeautifulSoup(html, "lxml")
            [s.decompose() for s in soup("script")]
            if soup.body is None:
                return ('no body', None)
            body_text = self._RE_COMBINE_WHITESPACE.sub(" ", soup.body.get_text(' ')).strip()
            return self.predict_language(body_text, k=1)[0]
        except Exception as e:
            print(f"Error in get_url_language: {e}")
            return ('error', None)

    def clean_text_line_by_line(self, text: str, join: bool = True, ch_join: str = ' ') -> str:
        lines = [line.strip() for line in text.splitlines()]
        lines = [re.sub(r' {2,}', ' ', line) for line in lines]
        lines = [re.sub(r' ([.,:;!?\\-])', r'\1', line) for line in lines]
        lines = [line + '.' if line and line[-1] not in string.punctuation else line for line in lines]
        lines = [line for line in lines if line]
        return ch_join.join(lines) if join else lines

    def apply_manual_rules(self, text: str) -> str:
        return re.sub(r'\[[0-9]+\]', '', text)
    
    def retrieve_text_from_html(self, html: str, soup_parser: str = 'lxml') -> str:
        if not isinstance(html, str) or not any(tag in html.lower() for tag in ['<html', '<body', '<!doctype html']):
            return 'No body'
        
        try:
            soup = BeautifulSoup(html, soup_parser)
            for script in soup(["script", "style"]):
                script.decompose()
            
            content = soup.body if soup.body else soup
            
            for s in content.find_all('strong'):
                s.unwrap()
            
            for p in content.find_all('p'):
                p.string = self._RE_COMBINE_WHITESPACE.sub(" ", p.get_text('')).strip()
            
            text = content.get_text(' ').strip()
            text = self.apply_manual_rules(text)
            text = self.clean_text_line_by_line(text, ch_join=' ')
            
            return text if text else 'No body'
        
        except Exception as e:
            print(f"Error parsing HTML: {e}")
            return 'No body'

    def process_dataframe(self, reference_html_df):
        tqdm.pandas()
        reference_html_df['extracted_sentences'] = reference_html_df.html.progress_apply(
            lambda html: self.retrieve_text_from_html(html).split('\n')
        )
        reference_html_df['extracted_text'] = reference_html_df.extracted_sentences.apply(' '.join)
        reference_html_df['nlp_sentences'] = reference_html_df.extracted_text.progress_apply(
            lambda x: [str(s) for s in self.nlp(x).sents]
        )

        slide_config = self.config['text_processing']['sentence_slide']
        if slide_config['enabled']:
            window_size = slide_config.get('window_size', 2)
            join_char = slide_config.get('join_char', ' ')
            
            reference_html_df[f'nlp_sentences_slide_{window_size}'] = reference_html_df['nlp_sentences'].progress_apply(
                lambda x: [join_char.join(x[i:i+window_size]) for i in range(len(x) - window_size + 1)]
            )

        return reference_html_df
    
    

def html2text(html_set):
    processor = HTMLTextProcessor()
    result_df = processor.process_dataframe(html_set)
    
    return result_df

def main(qids: List[str]):
    config = load_config('config.yaml')  # Load config once
    with HTMLFetcher(config_path='config.yaml') as fetcher:
        if fetcher.reset:
            fetcher.reset_tables()
        url_references_df = fetcher.get_url_references(qids)
        fetcher.create_url_html_table(url_references_df)
        fetcher.fetch_and_update_html()
        for qid in qids:  # claim label getting
            html_set = fetcher.process_qid(qid)
            if len(html_set) != 0:
                claim_text = fetcher.claim2text(html_set)
                html_text = html2text(html_set)
                fetcher.store_data_to_db(claim_text, html_text)
            

            
if __name__ == "__main__":
    qids =['Q3095']
    main(qids)