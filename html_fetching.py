import sqlite3
import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any, Tuple, Union
import yaml, requests, time
from requests.exceptions import RequestException
import utils.wikidata_utils as wdutils
from tqdm import tqdm
import ast
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path: str = 'config.yaml') -> Dict[str, Any]:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

config = load_config()

class WikidataObjectProcessor:
    def __init__(self):
        self.Wd_API = wdutils.CachedWikidataAPI()
        self.Wd_API.languages = ['en']
        self.dt_types = ['wikibase-item', 'monolingualtext', 'quantity', 'time', 'string']

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

    def process_wikibase_item(self, dv: str, attr: str) -> Union[Tuple[str, str], List[str]]:
        item_id = ast.literal_eval(dv)['value']['id']
        if attr == 'label':
            return getattr(self.Wd_API, f'get_{attr}')(item_id, True)
        return getattr(self.Wd_API, f'get_{attr}')(item_id)

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

    def get_object_attribute(self, row: dict, attr: str) -> Union[Tuple[str, str], List[str]]:
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
    return WikidataObjectProcessor().get_object_attribute(row, 'label')

def get_object_desc_given_datatype(row: dict) -> Tuple[str, str]:
    return WikidataObjectProcessor().get_object_attribute(row, 'desc')

def get_object_alias_given_datatype(row: dict) -> Union[Tuple[str, str], List[str]]:
    return WikidataObjectProcessor().get_object_attribute(row, 'alias')

class HTMLFetcher:
    def __init__(self, db_name: str = 'wikidata_claims_refs_parsed.db', config: Dict[str, Any] = None):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        self.config = config or {}
        self.Wd_API = wdutils.CachedWikidataAPI()
        self.Wd_API.languages = ['en']
        self.BAD_DATATYPES = ['external-id', 'commonsMedia', 'url', 'globe-coordinate', 'wikibase-lexeme', 'wikibase-property']


    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.ensure_url_html_table()
        return self

    def ensure_url_html_table(self):
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS url_html (
                    url TEXT PRIMARY KEY,
                    html TEXT
                )
            ''')
            self.conn.commit()
            logging.info("Ensured url_html table exists")
        except sqlite3.Error as e:
            logging.error(f"An error occurred while ensuring url_html table: {e}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def reset_table(self):
        try:
            self.cursor.execute("DROP TABLE IF EXISTS url_html")
            self.conn.commit()
            self.ensure_url_html_table()
        except sqlite3.Error as e:
            logging.error(f"An error occurred while resetting url_html table: {e}")

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

    def fetch_and_update_html(self):
        batch_size = self.config.get('html_fetching', {}).get('batch_size', 20)
        delay = self.config.get('html_fetching', {}).get('delay', 1.0)

        try:
            self.cursor.execute("SELECT url FROM url_html WHERE html IS NULL")
            urls_to_fetch = self.cursor.fetchall()
            
            for i, (url,) in enumerate(urls_to_fetch):
                if i > 0 and i % batch_size == 0:
                    self.conn.commit()
                    time.sleep(delay)  # Delay to avoid overwhelming the server

                try:
                    response = requests.get(url, timeout=10)
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

    def add_labels_and_descriptions(self, claim_df: pd.DataFrame) -> pd.DataFrame:
        tqdm.pandas()
        for entity_type in ['entity', 'property']:
            for attr_type in ['label', 'alias', 'desc']:
                claim_df[[f'{entity_type}_{attr_type}', f'{entity_type}_{attr_type}_lan']] = pd.DataFrame(
                    claim_df[f'{entity_type}_id'].progress_apply(getattr(self.Wd_API, f'get_{attr_type}')).tolist()
                )

        claim_df['object_label'] = claim_df.apply(get_object_label_given_datatype, axis=1)
        claim_df['object_alias'] = claim_df.apply(get_object_alias_given_datatype, axis=1)
        claim_df['object_desc'] = claim_df.apply(get_object_desc_given_datatype, axis=1)

        for attr in ['label', 'alias', 'desc']:
            claim_df[f'object_{attr}'], claim_df[f'object_{attr}_lan'] = zip(*claim_df[f'object_{attr}'].apply(lambda x: x if isinstance(x, tuple) else (x, '')))
        return claim_df[claim_df['object_label_lan'] != 'none'].reset_index(drop=True)

    def process_qid(self, qid: str) -> pd.DataFrame:
        conn = sqlite3.connect(db_path)
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

    def claim2text(self, qid):       
        html_set = self.process_qid(qid)
        claim_df = self.process_claim_data(html_set)
        print(f"Number of unique reference IDs: {claim_df.reference_id.nunique()}")
        return self.add_labels_and_descriptions(claim_df)


def main(qids: List[str],  reset: bool):
    with HTMLFetcher(config=config) as fetcher:
        if reset:
            fetcher.reset_table()
        # Fetch URL references for multiple specific entities
        url_references_df = fetcher.get_url_references(qids)
        fetcher.create_url_html_table(url_references_df)
        ###fetcher.fetch_and_update_html()
        for qid in qids: #claim label getting
            claim_text = fetcher.claim2text(qid)
            
        

if __name__ == "__main__":
    qids_to_process = ["Q2", "Q42"]
    main(qids_to_process, config['parsing']['reset_database'])