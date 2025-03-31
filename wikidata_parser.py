import ast
import json
from typing import List, Dict, Any

from qwikidata.linked_data_interface import get_entity_dict_from_api
import nltk
import pandas as pd
import requests
import yaml

from utils.logger import logger


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
            Dictionary containing three DataFrames:
            - claims: All claim information
            - claims_refs: Claim-reference relationships
            - refs: All reference information
        """
        entity = get_entity_dict_from_api(qid)
        if not entity:
            logger.warning(f'Failed to fetch entity: {qid}')
            return {'claims': pd.DataFrame(), 'claims_refs': pd.DataFrame(), 'refs': pd.DataFrame()}
        
        claims_data = []
        claims_refs_data = []
        refs_data = []

        # Process all claims and references
        for claims in entity['claims'].values():
            for claim in claims:
                # Extract claim data
                mainsnak = claim['mainsnak']
                
                # Extract object_id and datavalue
                object_id = None
                if mainsnak['snaktype'] == 'value':
                    datavalue = mainsnak['datavalue']
                    if datavalue['type'] == 'wikibase-entityid':

                        if 'numeric-id' in datavalue['value']:
                            object_id = f"Q{datavalue['value']['numeric-id']}"
                        else:
                            object_id = str(datavalue['value'])
                    value = str(datavalue)
                else:
                    value = mainsnak['snaktype']
                
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
                    value,
                    object_id
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
                'property_id', 'datatype', 'datavalue', 'object_id'
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
        logger.info(f"Total claims: {original_size}")
        logger.info(f"Claims after filtering: {len(df)}")
        logger.info(f"Percentage kept: {len(df)/original_size*100:.2f}%")

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
                logger.warning(f"No formatter URL found for {property_id}")
                return 'no_formatter_url'
            return results['results']['bindings'][0]['formatter_url']['value']
            
        except requests.Timeout:
            logger.error(f"Timeout while fetching formatter URL for {property_id}")
            return 'no_formatter_url'
        except requests.RequestException as e:
            logger.error(f"Request error for {property_id}: {e}")
            return 'no_formatter_url'
        except Exception as e:
            logger.error(f"Unexpected error for {property_id}: {e}")
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
                logger.info("No data to process")
                return pd.DataFrame()

            # Get references for filtered claims
            valid_claim_ids = claims_df['claim_id'].unique()
            valid_refs = claims_refs_df[claims_refs_df['claim_id'].isin(valid_claim_ids)]
            
            if valid_refs.empty:
                logger.info("No valid references found")
                return pd.DataFrame()

            # Process URLs from valid references
            valid_ref_ids = valid_refs['reference_id'].unique()
            refs_df = refs_df[refs_df['reference_id'].isin(valid_ref_ids)]
            
            url_data = self._process_reference_urls(refs_df)
            
            if not url_data.empty:
                logger.info(f"Processed {len(url_data)} URL references")
            else:
                logger.info("No valid URLs found")

            return url_data

        except Exception as e:
            logger.error(f"Error in URL processing: {e}")
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

        # Process URL DataFrame
        url_data = []
        
        if not url_df.empty:
            url_data.append(url_df[['reference_id', 'reference_property_id', 
                                  'reference_index', 'reference_datatype', 'url']])
        
        # Process external ID DataFrame
        if not ext_id_df.empty and 'formatter_url' in ext_id_df.columns:
            valid_ext_ids = ext_id_df[ext_id_df['formatter_url'] != 'no_formatter_url'].copy()
            if not valid_ext_ids.empty:
                valid_ext_ids['url'] = valid_ext_ids.apply(
                    lambda x: x['formatter_url'].replace('$1', x['ext_id']), 
                    axis=1
                )
                url_data.append(valid_ext_ids[['reference_id', 'reference_property_id', 
                                             'reference_index', 'reference_datatype', 'url']])
        
        # Combine all URL data
        if not url_data:
            return pd.DataFrame()
        
        all_url_df = pd.concat(url_data, ignore_index=True)
        
        # Apply filters and sorting
        all_url_df = all_url_df.sort_values(['reference_id', 'reference_index']).reset_index(drop=True)
        
        # Get references with single URL
        reference_id_counts = all_url_df.reference_id.value_counts()
        single_url_references = reference_id_counts[reference_id_counts == 1].index
        url_data = all_url_df[all_url_df.reference_id.isin(single_url_references)]
        
        if not url_data.empty:
            url_data = url_data.drop('reference_index', axis=1).reset_index(drop=True)
            
        return url_data

    def get_labels_from_sparql(self, entity_ids: List[str]) -> Dict[str, str]:
        """
        Get labels for entities using SPARQL
        """
        endpoint_url = "https://query.wikidata.org/sparql"
        
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
                            headers=self.headers,
                            timeout=20)
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
            logger.error(f"Error fetching labels: {e}")
            return {}

class WikidataParser:
    def __init__(self, config_path: str = 'config.yaml'):
        self.config = Config(config_path)
        self.entity_processor = EntityProcessor()
        self.property_filter = PropertyFilter()
        self.url_processor = URLProcessor()
        self.processing_stats = {}  # Dictionary to store processing statistics

    def process_entity(self, qid: str) -> Dict[str, pd.DataFrame]:
        """Process a single entity with its QID."""
        try:
            # Track statistics without affecting the return structure
            self.processing_stats = {
                'entity_id': qid,
                'parsing_start_timestamp': pd.Timestamp.now(),
                'total_claims': 0,
                'filtered_claims': 0,
                'percentage_kept': 0.0,
                'url_references': 0
            }
            
            logger.info(f"Starting to process entity: {qid}")
            
            entity_data = self.entity_processor.process_entity(qid)
            total_claims = len(entity_data['claims'])
            self.processing_stats['total_claims'] = total_claims
            
            filtered_claims = self.property_filter.filter_properties(entity_data['claims'])
            filtered_claims_count = len(filtered_claims)
            self.processing_stats['filtered_claims'] = filtered_claims_count
            self.processing_stats['percentage_kept'] = (filtered_claims_count / total_claims * 100) if total_claims > 0 else 0
            
            # Fix "No label" entity labels
            if not filtered_claims.empty and filtered_claims['entity_label'].iloc[0].startswith('No label'):
                # Get the unique entity_id
                entity_id = filtered_claims['entity_id'].iloc[0]
                
                # Fetch the label using SPARQL
                missing_labels = self.url_processor.get_labels_from_sparql([entity_id])
                
                # Update the label if it exists
                if entity_id in missing_labels:
                    filtered_claims['entity_label'] = missing_labels[entity_id]
            
            result = {
                'claims': filtered_claims,
                'claims_refs': entity_data['claims_refs'],
                'refs': entity_data['refs']
            }
            
            url_data = self.url_processor.process_urls(result)
            self.processing_stats['url_references'] = len(url_data)
            
            result['urls'] = url_data
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to process entity {qid}: {str(e)}", exc_info=True)
            raise

    # Add new method to access statistics
    def get_processing_stats(self) -> Dict:
        """Return the processing statistics from the last entity processed"""
        return self.processing_stats



if __name__ == "__main__":
    nltk.download('punkt', quiet=True)

    parser = WikidataParser()
    result = parser.process_entity('Q51896665')
    stats = parser.get_processing_stats()#result.keys() = dict_keys(['claims', 'claims_refs', 'refs', 'urls'])


