import pandas as pd
import nltk
import html2text
import logging
import requests
from typing import Dict, List, Tuple, Optional
from utils.verbalisation_module import VerbModule
from utils.sentence_retrieval_module import SentenceRetrievalModule
import numpy as np

class HTMLSentenceProcessor:
    def __init__(self):
        nltk.download('punkt', quiet=True)
        self.logger = logging.getLogger(__name__)
        self.h = html2text.HTML2Text()
        self.h.ignore_links = True

    def process_html_to_sentences(self, html_df: pd.DataFrame) -> pd.DataFrame:
        """Convert HTML documents to sentences, skipping failed HTML fetches"""
        # Filter out failed HTML fetches
        valid_html_df = html_df[~html_df['html'].str.startswith('Error:')].copy()
        
        def split_into_sentences(text):
            if not text:
                return ["No TEXT"]
            return nltk.sent_tokenize(text)

        def slide_sentences(sentences, window_size=2):
            if not sentences:
                return ["No TEXT"]
            try:
                if len(sentences) < window_size:
                    return [" ".join(sentences)]
                return [" ".join(sentences[i:i + window_size]) for i in range(len(sentences) - window_size + 1)]
            except:
                return ["No TEXT"]
        
        # Convert HTML to text using html2text
        valid_html_df['html2text'] = valid_html_df['html'].apply(lambda x: self.h.handle(x))

        # Split text into sentences
        valid_html_df['nlp_sentences'] = valid_html_df['html2text'].apply(split_into_sentences)
        valid_html_df['nlp_sentences_slide_2'] = valid_html_df['nlp_sentences'].apply(slide_sentences)

        return valid_html_df[['reference_id', 'url', 'nlp_sentences', 'nlp_sentences_slide_2']]

class EvidenceSelector:
    def __init__(self, sentence_retrieval=None, verb_module=None):
        self.logger = logging.getLogger(__name__)
        self.endpoint_url = "https://query.wikidata.org/sparql"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; MyBot/1.0; mailto:your@email.com)'
        }
        # Use provided models or create new ones
        self.verb_module = verb_module or VerbModule()
        self.sentence_retrieval = sentence_retrieval or SentenceRetrievalModule(max_len=512)
        self.top_k = 5

    def get_labels_from_sparql(self, property_ids: List[str], entity_ids: List[str]) -> Tuple[Dict[str, str], Dict[str, str]]:
        """
        Get labels for properties and entities using SPARQL
        """
        # Prepare property and entity IDs for SPARQL query
        property_values = ' '.join([f'wd:{pid}' for pid in property_ids])
        entity_values = ' '.join([f'wd:{eid}' for eid in entity_ids])
        
        query = f"""
        SELECT ?id ?label WHERE {{
          VALUES ?id {{ {property_values} {entity_values} }}
          ?id rdfs:label ?label .
          FILTER(LANG(?label) = "en")
        }}
        """
        
        try:
            r = requests.get(self.endpoint_url, 
                           params={'format': 'json', 'query': query},
                           headers=self.headers)
            r.raise_for_status()
            results = r.json()
            
            # Create dictionaries for property and entity labels
            labels = {}
            for result in results['results']['bindings']:
                entity_id = result['id']['value'].split('/')[-1]
                label = result['label']['value']
                labels[entity_id] = label
                
            return labels
            
        except Exception as e:
            self.logger.error(f"Error fetching labels: {e}")
            return {}

    def extract_object_id(self, datavalue: str) -> Optional[str]:
        """Extract object ID from datavalue string"""
        try:
            value_dict = eval(datavalue)
            if 'value' in value_dict and 'numeric-id' in value_dict['value']:
                return f"Q{value_dict['value']['numeric-id']}"
        except:
            pass
        return None

    def enrich_claims_with_labels(self, relevant_claims: pd.DataFrame) -> pd.DataFrame:
        """Add property and object labels to claims"""
        # Get unique property IDs and entity IDs
        property_ids = relevant_claims['property_id'].unique().tolist()
        
        # Extract object IDs from datavalue
        relevant_claims['object_id'] = relevant_claims['datavalue'].apply(self.extract_object_id)
        object_ids = [oid for oid in relevant_claims['object_id'].unique() if oid is not None]
        
        # Get labels from SPARQL
        all_labels = self.get_labels_from_sparql(property_ids, object_ids)
        
        # Add labels as new columns
        relevant_claims['property_label'] = relevant_claims['property_id'].map(all_labels)
        relevant_claims['object_label'] = relevant_claims['object_id'].map(all_labels)
        
        return relevant_claims

    def get_relevant_claims(self, sentences_df: pd.DataFrame, claims_df: pd.DataFrame, claims_refs_df: pd.DataFrame) -> pd.DataFrame:
        """
        Find claims that have accessible references and enrich them with labels
        """
        # Get list of reference_ids that we actually have sentences for
        accessible_refs = set(sentences_df['reference_id'].unique())
        
        # Filter claims_refs to only include references we have sentences for
        valid_claims_refs = claims_refs_df[claims_refs_df['reference_id'].isin(accessible_refs)]
        
        # Get the claims and merge with their accessible references
        relevant_claims = (claims_df[['claim_id', 'entity_id', 'property_id', 'datavalue', 'entity_label']]
                          .merge(valid_claims_refs[['claim_id', 'reference_id']], 
                                on='claim_id', 
                                how='inner'))
        
        # Rename entity_id column to qid for consistency with the rest of the code
        relevant_claims = relevant_claims.rename(columns={'entity_id': 'qid'})
        
        # Enrich claims with property and object labels
        relevant_claims = self.enrich_claims_with_labels(relevant_claims)
        
        return relevant_claims

    def verbalize_claims(self, relevant_claims: pd.DataFrame) -> pd.DataFrame:
        """
        Add verbalized versions of the claims to the DataFrame
        """
        # Create triples for verbalization
        triples = []
        for _, row in relevant_claims.iterrows():
            triple = {
                'subject': row['entity_label'],
                'predicate': row['property_label'],
                'object': row['object_label']
            }
            triples.append(triple)
        
        # Add verbalization columns
        relevant_claims['verbalisation'] = self.verb_module.verbalise_triples(triples)
        relevant_claims['verbalisation_unks_replaced'] = relevant_claims['verbalisation'].apply(
            self.verb_module.replace_unks_on_sentence
        )
        relevant_claims['verbalisation_unks_replaced_then_dropped'] = relevant_claims['verbalisation'].apply(
            lambda x: self.verb_module.replace_unks_on_sentence(x, empty_after=True)
        )
        
        return relevant_claims

    def select_relevant_sentences(self, relevant_claims: pd.DataFrame, sentences_df: pd.DataFrame) -> pd.DataFrame:
        """
        Select most relevant sentences for each claim using semantic similarity
        """
        results = []
        
        for _, claim_row in relevant_claims.iterrows():
            claim_text = claim_row['verbalisation_unks_replaced_then_dropped']
            ref_id = claim_row['reference_id']
            
            # Get sentences for the matching reference_id
            ref_sentences = sentences_df[sentences_df['reference_id'] == ref_id]['nlp_sentences'].iloc[0]
            
            if not ref_sentences or ref_sentences == ["No TEXT"]:
                continue
                
            # Create sentence pairs for scoring
            sentence_pairs = [(claim_text, sentence) for sentence in ref_sentences]
            
            # Get similarity scores using score_sentence_pairs
            similarities = self.sentence_retrieval.score_sentence_pairs(sentence_pairs)
            
            # Get top k most similar sentences
            top_k_indices = np.argsort(similarities)[-self.top_k:][::-1]
            
            # Create results for this claim
            for idx in top_k_indices:
                score = float(similarities[idx])
                sentence = ref_sentences[idx]
                results.append({
                    'reference_id': ref_id,
                    'claim_id': claim_row['claim_id'],
                    'claim': claim_text,
                    'sentence': sentence,
                    'similarity_score': score,
                    'sentence_id': f"{ref_id}_{idx}",
                    'qid': claim_row['qid'],
                    'property_id': claim_row['property_id'],
                    'object_id': claim_row['object_id'],
                    'entity_label': claim_row['entity_label'],
                    'property_label': claim_row['property_label'],
                    'object_label': claim_row['object_label']
                })
        
        return pd.DataFrame(results)

    def process_evidence(self, sentences_df: pd.DataFrame, parser_result: Dict) -> pd.DataFrame:
        """
        Main method to process evidence selection pipeline
        
        Args:
            sentences_df: DataFrame containing processed sentences
            parser_result: Dictionary containing 'claims' and 'claims_refs' DataFrames
        
        Returns:
            DataFrame containing selected evidence sentences with similarity scores
        """
        # 1. Get relevant claims with references
        relevant_claims = self.get_relevant_claims(
            sentences_df, 
            parser_result['claims'], 
            parser_result['claims_refs']
        )
        
        # 2. Add verbalization
        relevant_claims = self.verbalize_claims(relevant_claims)
        
        # 3. Select relevant sentences
        evidence_df = self.select_relevant_sentences(relevant_claims, sentences_df)
        
        return evidence_df

if __name__ == "__main__":
    qid = 'Q107405554'

    # Get URLs and claims from WikidataParser
    from wikidata_parser import WikidataParser
    from refs_html_collection import HTMLFetcher
    
    # Get URLs and claims
    parser = WikidataParser()
    parser_result = parser.process_entity(qid)
    
    # Fetch HTML content
    fetcher = HTMLFetcher(config_path='config.yaml')
    html_df = fetcher.fetch_all_html(parser_result['urls'], parser_result)

    # Convert HTML to sentences
    processor = HTMLSentenceProcessor()
    sentences_df = processor.process_html_to_sentences(html_df)
    
    # Process evidence selection
    selector = EvidenceSelector()
    evidence_df = selector.process_evidence(sentences_df, parser_result)
    





