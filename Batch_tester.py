import sqlite3
import pandas as pd
import Wikidata_Text_Parser as wtr
import Prove_lite as prv
import gzip, traceback, json, os
from tqdm import tqdm
import pickle

class DatabaseExtractor():
    def __init__(self, dbname='wikidata_claims_refs_parsed.db'):
        self.dbname = dbname
        self.prepare_extraction()
        
    def finish_extraction(self):
        self.db.commit()
        self.db.close()
        
    def prepare_extraction(self):
        self.db = sqlite3.connect(self.dbname)
        self.cursor = self.db.cursor()

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS claims(
                entity_id TEXT,
                claim_id TEXT,
                rank TEXT,
                property_id TEXT,
                datatype TEXT,
                datavalue TEXT,
                PRIMARY KEY (
                    claim_id
                )
        )''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS claims_refs(
                claim_id TEXT,
                reference_id TEXT,
                PRIMARY KEY (
                    claim_id,
                    reference_id
                )
        )''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS refs(
                reference_id TEXT,
                reference_property_id TEXT,
                reference_index TEXT,
                reference_datatype TEXT,
                reference_value TEXT,
                PRIMARY KEY (
                    reference_id,
                    reference_property_id,
                    reference_index
                )
        )''')
        self.db.commit()  
        
    def extract_claim(self, entity_id, claim):
        if claim['mainsnak']['snaktype'] == 'value':
            value = str(claim['mainsnak']['datavalue'])
        else:
            value = claim['mainsnak']['snaktype']
        try:
            self.cursor.execute('''
            INSERT INTO claims(entity_id, claim_id, rank, property_id, datatype, datavalue)
            VALUES($var,$var,$var,$var,$var,$var)'''.replace('$var','?'), (
                entity_id,claim['id'],claim['rank'],
                claim['mainsnak']['property'],claim['mainsnak']['datatype'],value
            ))
        except UnicodeEncodeError:
            print(entity_id,claim['id'],claim['rank'],
                claim['mainsnak']['property'],claim['mainsnak']['datatype'],value)
            raise
        except sqlite3.IntegrityError as err:
            #self.db.rollback()
            self.cursor.execute(
                '''SELECT *
                FROM claims 
                WHERE claim_id=$var
                '''.replace('$var','?'), (claim['id'],)
            )
            conflicted_value = self.cursor.fetchone()
            if conflicted_value == (entity_id,claim['id'],claim['rank'],
                    claim['mainsnak']['property'],claim['mainsnak']['datatype'],value):
                pass
            else:
                print(err, claim['id'])
                traceback.print_exc()
                raise err
        finally:
            #self.db.commit()
            pass

    def extract_reference(self, ref):
        for snaks in ref['snaks'].values():
            for i, snak in enumerate(snaks):
                if snak['snaktype'] == 'value':
                    value = str(snak['datavalue'])
                else:
                    value = snak['snaktype']
                try:
                    self.cursor.execute('''
                    INSERT INTO refs(reference_id, reference_property_id, reference_index,
                    reference_datatype, reference_value)
                    VALUES($var,$var,$var,$var,$var)'''.replace('$var','?'), (
                        ref['hash'],snak['property'],str(i),snak['datatype'],value
                    ))
                except sqlite3.IntegrityError as err:
                    #self.db.rollback()
                    self.cursor.execute(# WE DONT USE THE INDEX HERE, THEY TEND TO COME SHUFFLED FROM API AND SORTING TAKES TOO LONG
                        '''SELECT reference_id, reference_property_id, reference_datatype, reference_value
                        FROM refs 
                        WHERE reference_id = $var
                        AND reference_property_id = $var
                        '''.replace('$var','?'), (ref['hash'],snak['property'])
                    )
                    conflicted_values = self.cursor.fetchall()
                    if  (ref['hash'],snak['property'],snak['datatype'],value) in conflicted_values:
                        pass
                    else:
                        print(err, ref['hash'],snak['property'],i)
                        print('trying to insert:',(ref['hash'],snak['property'],str(i),snak['datatype'],value))
                        traceback.print_exc()
                        raise err
                finally:
                    #self.db.commit()
                    pass
            
    def extract_claim_reference(self, claim, ref):
        claim['id'],ref['hash']
        try:
            self.cursor.execute('''
            INSERT INTO claims_refs(claim_id, reference_id)
            VALUES($var,$var)'''.replace('$var','?'), (
                claim['id'],ref['hash']
            ))
        except sqlite3.IntegrityError as err:
            #db.rollback()
            pass
        finally:
            #self.db.commit()
            pass
    
    def extract_entity(self, e):
        for outgoing_property_id in e['claims'].values():
            for claim in outgoing_property_id:
                self.extract_claim(e['id'],claim)
                if 'references' in claim:
                    for ref in claim['references']: 
                        self.extract_claim_reference(claim, ref)
                        self.extract_reference(ref)

def process_wikidata_dump(file_path, max_entities=None):
    entities_processed = 0
    
    with gzip.open(file_path, 'rt', encoding='utf-8') as dump_file:
        for line in tqdm(dump_file, desc="Processing entities"):
            line = line.strip()
            
            if line.startswith('{') and line.endswith('}') or \
               line.startswith('{') and line.endswith('},'):
                
                if line.endswith('},'):
                    line = line[:-1]
                
                try:
                    entity = json.loads(line)
                    
                    # Extract relevant information
                    entity_id = entity['id']
                    entity_type = entity['type']
                    labels = entity.get('labels', {}).get('en', {})
                    descriptions = entity.get('descriptions', {}).get('en', {})
                    claims = entity.get('claims', {})
                    aliases = entity.get('aliases', {}).get('en', [])

                    # Get English entity label, alias, and description
                    entity_label = labels.get('value', '')
                    entity_alias = aliases[0]['value'] if aliases else ''
                    entity_desc = descriptions.get('value', '')

                    # Get property information (English only)
                    property_info = {}
                    for prop_id, prop_claims in claims.items():
                        if prop_claims:
                            prop_claim = prop_claims[0]  # Take the first claim for this property
                            prop_datatype = prop_claim['mainsnak']['datatype']
                            prop_label = prop_claim['mainsnak'].get('property', '')
                            property_info[prop_id] = {
                                'datatype': prop_datatype,
                                'label': prop_label,
                            }

                    # Create a simplified entity representation
                    simplified_entity = {
                        'id': entity_id,
                        'type': entity_type,
                        'label': entity_label,
                        'description': entity_desc,
                        'alias': entity_alias,
                        'claims': claims,
                        'property_info': property_info
                    }
                    
                    yield simplified_entity
                    
                    entities_processed += 1
                    if max_entities and entities_processed >= max_entities:
                        break
                
                except json.JSONDecodeError:
                    print(f"Error decoding JSON: {line}")
    
    print(f"Processed {entities_processed} entities")

# Usage example
dump_file_path = '../wikidata-20220103-all.json.gz'
max_entities_to_process = 100000  # Set to None to process all entities

def initialzeDB():
    db_file = 'wikidata_claims_refs_parsed.db'
    if os.path.exists(db_file):
        os.remove(db_file)


# Using the generator
dict ={}
for entity in process_wikidata_dump(dump_file_path, max_entities_to_process):
    extractor = DatabaseExtractor()
    conn = sqlite3.connect('wikidata_claims_refs_parsed.db')
    target_QID = entity["id"]
    if entity['type'] != 'item':
        initialzeDB()
        continue 
    print(f'Starting procesesing: {target_QID}')
    if entity:
        extractor.extract_entity(entity)
    else:
        print(f'Failed to fetch entity: {target_QID}')
    extractor.finish_extraction()
    try:
        filtered_df = wtr.propertyFiltering(target_QID) #update db and return dataframe after filtering
        url_set = wtr.urlParser(target_QID) #from ref table in .db
        html_set = wtr.htmlParser(url_set, target_QID) #Original html docs collection
        claim_text = wtr.claim2text(html_set) #Claims generation
        html_text = wtr.html2text(html_set)
        claim_text = claim_text.astype(str)
        html_text = html_text.astype(str)
        claim_text.to_sql('claim_text', conn, if_exists='replace', index=False)
        html_text.to_sql('html_text', conn, if_exists='replace', index=False)

        query = f"SELECT * FROM claim_text WHERE entity_id = '{target_QID}'"
        claim_df = pd.read_sql_query(query, conn)
        query = f"SELECT * FROM html_text Where  entity_id = '{target_QID}'"
        reference_text_df = pd.read_sql_query(query, conn)
        verbalised_claims_df_final = prv.verbalisation(claim_df)
        splited_sentences_from_html = prv.setencesSpliter(verbalised_claims_df_final, reference_text_df)
        BATCH_SIZE = 512
        N_TOP_SENTENCES = 5
        SCORE_THRESHOLD = 0.6
        evidence_df = prv.evidenceSelection(splited_sentences_from_html, BATCH_SIZE, N_TOP_SENTENCES)
        result = prv.textEntailment(evidence_df, SCORE_THRESHOLD)
        all_result, display_df = prv.TableMaking(verbalised_claims_df_final, result)
    except:
        print('there are no accessible urls or html documents')
    initialzeDB()
    dict[str(entity['id'])] = {'entity': entity, 'result': all_result}
    with open('CodeArchive/wikidata_stats.pickle', 'wb') as f:
        pickle.dump(dict, f)

