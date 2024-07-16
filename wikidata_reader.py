import sqlite3
import pandas as pd
from qwikidata.linked_data_interface import get_entity_dict_from_api
import nltk
import spacy
import logging
from typing import List, Dict, Any
import sys, subprocess
import yaml, json, ast
import utils.wikidata_utils as wdutils


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ensure_spacy_model():
    try:
        spacy.load("en_core_web_sm")
    except OSError:
        logging.info("Downloading spaCy model...")
        subprocess.check_call([sys.executable, "-m", "spacy", "download", "en_core_web_sm"])

def load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

class WikidataParser:
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = load_config(config_path)
        self.db_name = self.config.get('database', {}).get('name')
        self.conn = None
        self.cursor = None
        self.nlp = None
        self.Wd_API = wdutils.CachedWikidataAPI()
        self.Wd_API.languages = ['en']
        self.reset = self.config.get('parsing', {}).get('reset_database')

    
    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.setup_database()
        ensure_spacy_model()
        self.nlp = spacy.load("en_core_web_sm")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def setup_database(self):
        self.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS claims(
                entity_id TEXT,
                claim_id TEXT,
                rank TEXT,
                property_id TEXT,
                datatype TEXT,
                datavalue TEXT,
                PRIMARY KEY (claim_id)
            );
            
            CREATE TABLE IF NOT EXISTS claims_refs(
                claim_id TEXT,
                reference_id TEXT,
                PRIMARY KEY (claim_id, reference_id)
            );
            
            CREATE TABLE IF NOT EXISTS refs(
                reference_id TEXT,
                reference_property_id TEXT,
                reference_index TEXT,
                reference_datatype TEXT,
                reference_value TEXT,
                PRIMARY KEY (reference_id, reference_property_id, reference_index)
            );
                                  
            CREATE TABLE IF NOT EXISTS filtered_claims(
            entity_id TEXT,
            claim_id TEXT,
            rank TEXT,
            property_id TEXT,
            datatype TEXT,
            datavalue TEXT,
            PRIMARY KEY (claim_id)
            );
                                  
            CREATE TABLE IF NOT EXISTS url_references(
                entity_id TEXT,
                reference_id TEXT,
                reference_property_id TEXT,
                reference_datatype TEXT,
                url TEXT,
                PRIMARY KEY (entity_id, reference_id, reference_property_id)
            );
                                  
        ''')
        self.conn.commit()

    def finish_extraction(self):
        if self.conn:
            self.conn.commit()
        else:
            logging.warning("No database connection to commit")

    def reset_database(self):
        """Drops all tables and recreates them."""
        logging.info("Resetting database...")
        self.cursor.executescript('''
            DROP TABLE IF EXISTS claims;
            DROP TABLE IF EXISTS claims_refs;
            DROP TABLE IF EXISTS refs;
        ''')
        self.setup_database()
        logging.info("Database reset completed.")

    def extract_claim(self, entity_id, claim):
        mainsnak = claim['mainsnak']
        value = str(mainsnak['datavalue']) if mainsnak['snaktype'] == 'value' else mainsnak['snaktype']
        
        claim_data = (
            entity_id, claim['id'], claim['rank'],
            mainsnak['property'], mainsnak['datatype'], value
        )

        try:
            self.cursor.execute('''
                INSERT INTO claims(entity_id, claim_id, rank, property_id, datatype, datavalue)
                VALUES(?, ?, ?, ?, ?, ?)
            ''', claim_data)
            
        except sqlite3.IntegrityError:
            self._handle_integrity_error(claim_data)
            
        except UnicodeEncodeError:
            logging.error(f"UnicodeEncodeError for claim: {claim_data}")
            raise
        
        except sqlite3.Error as e:
            logging.error(f"SQLite error: {e}")
            raise

    def _handle_integrity_error(self, claim_data):
        self.cursor.execute(
            'SELECT * FROM claims WHERE claim_id = ?', 
            (claim_data[1],)  # claim_id is the second element
        )
        existing_claim = self.cursor.fetchone()
        
        if existing_claim == claim_data:
            logging.info(f"Duplicate claim ignored: {claim_data[1]}")
        else:
            logging.error(f"Integrity error for claim: {claim_data[1]}")
            logging.error(f"Existing: {existing_claim}")
            logging.error(f"New: {claim_data}")
            raise sqlite3.IntegrityError(f"Conflicting data for claim: {claim_data[1]}")
    
    def extract_claim_reference(self, claim, ref):
        try:
            self.cursor.execute('''
                INSERT OR IGNORE INTO claims_refs(claim_id, reference_id)
                VALUES(?, ?)
            ''', (claim['id'], ref['hash']))
        except sqlite3.Error as err:
            logging.error(f"Error inserting claim_reference: {err}")
            logging.error(f"Claim ID: {claim['id']}, Reference Hash: {ref['hash']}")

    def extract_reference(self, ref):
        for property_id, snaks in ref['snaks'].items():
            for i, snak in enumerate(snaks):
                value = str(snak['datavalue']) if snak['snaktype'] == 'value' else snak['snaktype']
                ref_data = (ref['hash'], property_id, str(i), snak['datatype'], value)
                
                try:
                    self.cursor.execute('''
                        INSERT INTO refs(reference_id, reference_property_id, reference_index,
                        reference_datatype, reference_value)
                        VALUES(?, ?, ?, ?, ?)
                    ''', ref_data)
                except sqlite3.IntegrityError:
                    self._handle_reference_integrity_error(ref_data)
                except sqlite3.Error as err:
                    logging.error(f"Error inserting reference: {err}")
                    logging.error(f"Reference data: {ref_data}")

    def _handle_reference_integrity_error(self, ref_data):
        self.cursor.execute('''
            SELECT reference_id, reference_property_id, reference_datatype, reference_value
            FROM refs
            WHERE reference_id = ? AND reference_property_id = ?
        ''', (ref_data[0], ref_data[1]))
        
        existing_refs = self.cursor.fetchall()
        if (ref_data[0], ref_data[1], ref_data[3], ref_data[4]) in existing_refs:
            logging.info(f"Duplicate reference ignored: {ref_data[0]}, {ref_data[1]}")
        else:
            logging.error(f"Integrity error for reference: {ref_data[0]}, {ref_data[1]}")
            logging.error(f"Existing: {existing_refs}")
            logging.error(f"New: {ref_data}")
            raise sqlite3.IntegrityError(f"Conflicting data for reference: {ref_data[0]}, {ref_data[1]}")

    def extract_entity(self, e):
        for outgoing_property_id in e['claims'].values():
            for claim in outgoing_property_id:
                self.extract_claim(e['id'],claim)
                if 'references' in claim:
                    for ref in claim['references']: 
                        self.extract_claim_reference(claim, ref)
                        self.extract_reference(ref)


    def claimParser(self, qid):
        entity_id = qid
        logging.info('Fetching entity from API ...')
        entity = get_entity_dict_from_api(entity_id)

        if entity:
            logging.info(f'Parsing entity: {entity_id}')
            self.extract_entity(entity)
        else:
            logging.warning(f'Failed to fetch entity: {entity_id}')

        self.conn.commit()

    def load_properties_to_remove(self, file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)
        return [item['id'] for item in data['general']]

    def propertyFiltering(self, QID):
        properties_to_remove = self.load_properties_to_remove('properties_to_remove.json')
        
        bad_datatypes = ['commonsMedia', 'external-id', 'globe-coordinate', 'url', 'wikibase-form',
                        'geo-shape', 'math', 'musical-notation', 'tabular-data', 'wikibase-sense']

        with sqlite3.connect(self.db_name) as conn:
            query = f"SELECT * FROM claims WHERE entity_id = ?"
            df = pd.read_sql_query(query, conn, params=(QID,))

            original_size = len(df)

            # Apply filters
            df = df[df['rank'] != 'deprecated']
            df = df[~df['datatype'].isin(bad_datatypes)]
            df = df[~df['property_id'].isin(properties_to_remove)]
            df = df[~df['datavalue'].isin(['somevalue', 'novalue'])]

            # Log filtering results
            print(f"Total claims for {QID}: {original_size}")
            print(f"Claims after filtering: {len(df)}")
            print(f"Percentage kept: {len(df)/original_size*100:.2f}%")

            # Remove old entries for this QID before inserting new ones
            cursor = conn.cursor()
            cursor.execute("DELETE FROM filtered_claims WHERE entity_id = ?", (QID,))

            # Append new filtered data to the table
            df.to_sql('filtered_claims', conn, if_exists='append', index=False)

    def reference_value_to_url(self, reference_value):
        if reference_value in ['novalue', 'somevalue']:
            return reference_value
        reference_value = ast.literal_eval(reference_value)
        assert reference_value['type'] == 'string'
        return reference_value['value']

    def get_formatter_url(self, entity_id):
        sparql_query = '''
            SELECT ?item WHERE {
                wd:$1 wdt:P1630 ?item.
            }
        '''.replace('$1', entity_id)
        sparql_results = self.Wd_API.query_sparql_endpoint(sparql_query)
        if sparql_results['results']['bindings']:
            return sparql_results['results']['bindings'][0]['item']['value']
        return 'no_formatter_url'
    
    def urlParser(self, qid):
        with sqlite3.connect(self.db_name) as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                # Get URL references by matching qid
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM filtered_claims WHERE entity_id = ?", (qid,))
                filtered_claims_rows = cursor.fetchall()

                claim_ids = [row[1] for row in filtered_claims_rows]
                placeholder = ', '.join(['?'] * len(claim_ids))
                query = f"SELECT * FROM claims_refs WHERE claim_id IN ({placeholder})"
                cursor.execute(query, claim_ids)
                claim_refs_rows = cursor.fetchall()
                
                reference_ids = [row[1] for row in claim_refs_rows]
                placeholder = ', '.join(['?'] * len(reference_ids))
                query = f"SELECT * FROM refs WHERE reference_id IN ({placeholder})"
                cursor.execute(query, reference_ids)
                refs_rows = cursor.fetchall()
                refs_df = pd.DataFrame(refs_rows, columns=[desc[0] for desc in cursor.description])

                # Get URL references
                url_df = refs_df[refs_df['reference_datatype'] == 'url'].copy()
                if not url_df.empty:
                    url_df['url'] = url_df.reference_value.apply(self.reference_value_to_url)
                else:
                    url_df['url'] = None

                # Get external ID references
                ext_id_df = refs_df[refs_df['reference_datatype'] == 'external-id'].copy()
                ext_id_df['ext_id'] = ext_id_df.reference_value.apply(self.reference_value_to_url)
                ext_id_df['formatter_url'] = ext_id_df['reference_property_id'].apply(self.get_formatter_url)
                if not ext_id_df.empty:
                    ext_id_df['url'] = ext_id_df.apply(lambda x: x['formatter_url'].replace('$1', x['ext_id']), axis=1)
                else:
                    ext_id_df['url'] = None
                conn.commit()
            except Exception as e:
                conn.rollback()
                logging.error(f"Error in urlParser: {e}")
                raise

        # Combine and clean up
        columns_for_join = ['reference_id', 'reference_property_id', 'reference_index', 'reference_datatype', 'url']
        all_url_df = pd.concat([url_df[columns_for_join], ext_id_df[columns_for_join]])
        all_url_df = all_url_df.sort_values(['reference_id', 'reference_index']).reset_index(drop=True)

        # Filter out invalid URLs
        all_url_df = all_url_df[~all_url_df['url'].isin(['no_formatter_url', 'somevalue', 'novalue'])]

        # Keep only references with a single URL
        reference_id_counts = all_url_df.reference_id.value_counts()
        single_url_references = reference_id_counts[reference_id_counts == 1].index
        all_url_df_eq1 = all_url_df[all_url_df.reference_id.isin(single_url_references)]

        # Prepare data for insertion
        url_data = all_url_df_eq1.drop('reference_index', axis=1).reset_index(drop=True)
        url_data['entity_id'] = qid

        # Insert data into the new table, ignoring duplicates
        cursor = conn.cursor()
        for _, row in url_data.iterrows():
            cursor.execute('''
                INSERT OR IGNORE INTO url_references 
                (entity_id, reference_id, reference_property_id, reference_datatype, url)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['entity_id'], row['reference_id'], row['reference_property_id'], 
                    row['reference_datatype'], row['url']))

        conn.commit()
        logging.info(f"Processed {len(url_data)} URL references for entity {qid}")

def main(qids: List[str], reset: bool = False):
    with WikidataParser() as parser:
        if parser.reset:
            parser.reset_database()
        for qid in qids: #batch processing to find claim informaton from Wikdiata
            parser.claimParser(qid)
        for qid in qids: #batch processing to clean proerpty of database
            parser.propertyFiltering(qid)
        for qid in qids: #batch processing to formatting urls 
            parser.urlParser(qid)

if __name__ == "__main__":
    nltk.download('punkt', quiet=True)
    qids =['Q3095']
    main(qids)