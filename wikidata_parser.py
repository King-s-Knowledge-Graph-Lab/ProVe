from qwikidata.linked_data_interface import get_entity_dict_from_api
import nltk
import spacy
import logging
from typing import List, Dict, Any, Optional, Union
import sys, subprocess
import yaml, json, ast
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Config:
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = self._load_config(config_path)
    
    @staticmethod
    def _load_config(config_path: str) -> Dict[str, Any]:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    @property
    def database_name(self) -> str:
        return self.config.get('database', {}).get('name')
    
    @property
    def reset_database(self) -> bool:
        return self.config.get('parsing', {}).get('reset_database', False)

class EntityProcessor:
    def process_entity(self, qid: str) -> Dict[str, pd.DataFrame]:
        """
        Process Wikidata entity claims and references and return them as DataFrames.
        
        Args:
            qid: Wikidata entity ID (e.g., 'Q44')
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary containing DataFrames for 
            'claims', 'claims_refs', 'refs'
        """
        entity = get_entity_dict_from_api(qid)
        if not entity:
            logging.warning(f'Failed to fetch entity: {qid}')
            return {'claims': pd.DataFrame(), 'claims_refs': pd.DataFrame(), 'refs': pd.DataFrame()}
        
        claims_data = []
        claims_refs_data = []
        refs_data = []

        # Process all claims and references
        for claims in entity['claims'].values():
            for claim in claims:
                # Extract claim data
                mainsnak = claim['mainsnak']
                value = str(mainsnak['datavalue']) if mainsnak['snaktype'] == 'value' else mainsnak['snaktype']
                
                entity_label = (
                    entity.get('labels', {})
                    .get('en', {})
                    .get('value', f"No label ({entity['id']})")
                )
                
                claims_data.append((
                    entity['id'],
                    entity_label,
                    claim['id'],
                    claim['rank'],
                    mainsnak['property'],
                    mainsnak['datatype'],
                    value
                ))

                # Extract reference data
                if 'references' in claim:
                    for ref in claim['references']:
                        claims_refs_data.append((claim['id'], ref['hash']))
                        
                        for prop_id, snaks in ref['snaks'].items():
                            for i, snak in enumerate(snaks):
                                value = str(snak['datavalue']) if snak['snaktype'] == 'value' else snak['snaktype']
                                refs_data.append((
                                    ref['hash'],
                                    prop_id,
                                    str(i),
                                    snak['datatype'],
                                    value
                                ))

        # Create and return DataFrames
        return {
            'claims': pd.DataFrame(claims_data, columns=[
                'entity_id', 'entity_label', 'claim_id', 'rank',
                'property_id', 'datatype', 'datavalue'
            ]),
            'claims_refs': pd.DataFrame(claims_refs_data, columns=[
                'claim_id', 'reference_id'
            ]),
            'refs': pd.DataFrame(refs_data, columns=[
                'reference_id', 'reference_property_id', 'reference_index',
                'reference_datatype', 'reference_value'
            ])
        }

class PropertyFilter:
    def __init__(self):
        self.bad_datatypes = [
            'commonsMedia', 'external-id', 'globe-coordinate', 'url', 
            'wikibase-form', 'geo-shape', 'math', 'musical-notation', 
            'tabular-data', 'wikibase-sense'
        ]

    def filter_properties(self, claims_df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter claims based on predefined rules
        
        Args:
            claims_df: DataFrame containing claims from EntityProcessor
            
        Returns:
            Filtered DataFrame of claims
        """
        if claims_df.empty:
            return claims_df
            
        original_size = len(claims_df)
        
        # Apply filters
        df = claims_df[claims_df['rank'] != 'deprecated']
        df = df[~df['datatype'].isin(self.bad_datatypes)]
        
        # Load and apply property filters
        properties_to_remove = self._load_properties_to_remove()
        df = df[~df['property_id'].isin(properties_to_remove)]
        
        # Filter out special values
        df = df[~df['datavalue'].isin(['somevalue', 'novalue'])]

        # Log filtering results
        logging.info(f"Total claims: {original_size}")
        logging.info(f"Claims after filtering: {len(df)}")
        logging.info(f"Percentage kept: {len(df)/original_size*100:.2f}%")

        return df

    def _load_properties_to_remove(self) -> List[str]:
        with open('properties_to_remove.json', 'r') as f:
            data = json.load(f)
        return [item['id'] for item in data['general']]

class URLProcessor:
    def __init__(self):
        self.sparql_endpoint = "https://query.wikidata.org/sparql"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; MyApp/1.0; mailto:your@email.com)'
        }

    def get_formatter_url(self, property_id: str) -> str:
        """Get formatter URL for external ID properties"""
        sparql_query = f"""
            SELECT ?formatter_url WHERE {{
                wd:{property_id} wdt:P1630 ?formatter_url.
            }}
        """
        
        try:
            response = requests.get(
                self.sparql_endpoint,
                params={
                    'query': sparql_query,
                    'format': 'json'
                },
                headers=self.headers,
                timeout=20
            )
            response.raise_for_status()
            
            results = response.json()
            if not results.get('results', {}).get('bindings'):
                logging.warning(f"No formatter URL found for {property_id}")
                return 'no_formatter_url'
            return results['results']['bindings'][0]['formatter_url']['value']
            
        except requests.Timeout:
            logging.error(f"Timeout while fetching formatter URL for {property_id}")
            return 'no_formatter_url'
        except requests.RequestException as e:
            logging.error(f"Request error for {property_id}: {e}")
            return 'no_formatter_url'
        except Exception as e:
            logging.error(f"Unexpected error for {property_id}: {e}")
            return 'no_formatter_url'

    def process_urls(self, filtered_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Process URL references from filtered claims and references
        
        Args:
            filtered_data: Dictionary containing filtered claims and references DataFrames
            
        Returns:
            DataFrame containing processed URL references
        """
        try:
            claims_df = filtered_data['claims']
            claims_refs_df = filtered_data['claims_refs']
            refs_df = filtered_data['refs']
            
            if claims_df.empty or refs_df.empty:
                logging.info("No data to process")
                return pd.DataFrame()

            # Get references for filtered claims
            valid_claim_ids = claims_df['claim_id'].unique()
            valid_refs = claims_refs_df[claims_refs_df['claim_id'].isin(valid_claim_ids)]
            
            if valid_refs.empty:
                logging.info("No valid references found")
                return pd.DataFrame()

            # Process URLs from valid references
            valid_ref_ids = valid_refs['reference_id'].unique()
            refs_df = refs_df[refs_df['reference_id'].isin(valid_ref_ids)]
            
            url_data = self._process_reference_urls(refs_df)
            
            if not url_data.empty:
                logging.info(f"Processed {len(url_data)} URL references")
            else:
                logging.info("No valid URLs found")

            return url_data

        except Exception as e:
            logging.error(f"Error in URL processing: {e}")
            raise

    def _process_reference_urls(self, refs_df: pd.DataFrame) -> pd.DataFrame:
        url_df = self._process_url_references(refs_df)
        ext_id_df = self._process_external_id_references(refs_df)
        
        # Combine and process URLs
        return self._combine_and_filter_urls(url_df, ext_id_df)

    def _process_url_references(self, refs_df: pd.DataFrame) -> pd.DataFrame:
        url_df = refs_df[refs_df['reference_datatype'] == 'url'].copy()
        if not url_df.empty:
            url_df['url'] = url_df.reference_value.apply(self._reference_value_to_url)
        return url_df

    def _process_external_id_references(self, refs_df: pd.DataFrame) -> pd.DataFrame:
        ext_id_df = refs_df[refs_df['reference_datatype'] == 'external-id'].copy()
        if not ext_id_df.empty:
            ext_id_df['ext_id'] = ext_id_df.reference_value.apply(self._reference_value_to_url)
            ext_id_df['formatter_url'] = ext_id_df['reference_property_id'].apply(self.get_formatter_url)
            ext_id_df['url'] = ext_id_df.apply(
                lambda x: x['formatter_url'].replace('$1', x['ext_id']) 
                if x['formatter_url'] != 'no_formatter_url' else 'placeholder',
                axis=1
            )
        return ext_id_df

    @staticmethod
    def _reference_value_to_url(reference_value: str) -> str:
        if reference_value in ['novalue', 'somevalue']:
            return reference_value
        reference_value = ast.literal_eval(reference_value)
        assert reference_value['type'] == 'string'
        return reference_value['value']

    def _combine_and_filter_urls(self, url_df: pd.DataFrame, ext_id_df: pd.DataFrame) -> pd.DataFrame:
        if url_df.empty and ext_id_df.empty:
            return pd.DataFrame()

        columns_for_join = ['reference_id', 'reference_property_id', 'reference_index', 
                           'reference_datatype', 'url']
        all_url_df = pd.concat([
            url_df[columns_for_join], 
            ext_id_df[columns_for_join]
        ], ignore_index=True)
        
        if all_url_df.empty:
            return all_url_df

        all_url_df = all_url_df.sort_values(['reference_id', 'reference_index']).reset_index(drop=True)
        all_url_df = all_url_df[~all_url_df['url'].isin(['placeholder', 'somevalue', 'novalue'])]

        reference_id_counts = all_url_df.reference_id.value_counts()
        single_url_references = reference_id_counts[reference_id_counts == 1].index
        url_data = all_url_df[all_url_df.reference_id.isin(single_url_references)]
        
        if not url_data.empty:
            url_data = url_data.drop('reference_index', axis=1).reset_index(drop=True)
            
        return url_data

class WikidataParser:
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = Config(config_path)
        self.entity_processor = EntityProcessor()
        self.property_filter = PropertyFilter()
        self.url_processor = URLProcessor()

    def process_entity(self, qid: str) -> Dict[str, pd.DataFrame]:
        """Process a single entity with its QID."""
        try:
            logging.info(f"Starting to process entity: {qid}")
            
            entity_data = self.entity_processor.process_entity(qid)
            logging.info(f"Entity data fetched: {len(entity_data['claims'])} claims")
            
            filtered_claims = self.property_filter.filter_properties(entity_data['claims'])
            logging.info(f"Claims filtered: {len(filtered_claims)} remaining")
            
            # Create filtered data dictionary
            filtered_data = {
                'claims': filtered_claims,
                'claims_refs': entity_data['claims_refs'],
                'refs': entity_data['refs']
            }
            
            # Process URLs
            url_data = self.url_processor.process_urls(filtered_data)
            
            # Add URL data to results
            filtered_data['urls'] = url_data
            
            logging.info(f"Entity {qid} processing completed successfully")
            return filtered_data
            
        except Exception as e:
            logging.error(f"Failed to process entity {qid}: {str(e)}", exc_info=True)
            raise

def ensure_spacy_model():
    try:
        spacy.load("en_core_web_sm")
    except OSError:
        logging.info("Downloading spaCy model...")
        subprocess.check_call([sys.executable, "-m", "spacy", "download", "en_core_web_sm"])



if __name__ == "__main__":
    nltk.download('punkt', quiet=True)
    qid = 'Q44'
    parser = WikidataParser()
    result = parser.process_entity('Q44') #result.keys() = dict_keys(['claims', 'claims_refs', 'refs', 'urls'])


