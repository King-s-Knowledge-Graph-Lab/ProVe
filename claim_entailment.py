import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging
import yaml
from utils.textual_entailment_module import TextualEntailmentModule
from tqdm import tqdm
from datetime import datetime

class ClaimEntailmentChecker:
    def __init__(self, config_path: str = 'config.yaml', text_entailment=None):
        self.logger = logging.getLogger(__name__)
        self.config = self.load_config(config_path)
        # Use provided model or create new one
        self.te_module = text_entailment or TextualEntailmentModule()
        
    @staticmethod
    def load_config(config_path: str) -> Dict:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    def check_entailment(self, evidence_df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform textual entailment checking on evidence sentences
        """
        SCORE_THRESHOLD = self.config['evidence_selection']['score_threshold']
        textual_entailment_df = evidence_df.copy()
        
        # Initialize columns for results
        te_columns = {
            'evidence_TE_prob': [],
            'evidence_TE_prob_weighted': [],
            'evidence_TE_labels': [],
            'claim_TE_prob_weighted_sum': [],
            'claim_TE_label_weighted_sum': [],
            'claim_TE_label_malon': [],
            'processed_timestamp': []
        }

        for _, row in tqdm(textual_entailment_df.iterrows(), total=textual_entailment_df.shape[0]):
            start_time = datetime.now()
            
            claim = row['claim']
            evidence = [{
                'sentence': row['sentence'],
                'score': row['similarity_score']
            }]
            evidence_size = len(evidence)
            
            # Get textual entailment probabilities
            evidence_TE_prob = self.te_module.get_batch_scores(
                claims=[claim] * evidence_size,
                evidence=[e['sentence'] for e in evidence]
            )
            
            # Get labels from probabilities
            evidence_TE_labels = [self.te_module.get_label_from_scores(s) for s in evidence_TE_prob]
            
            # Weight probabilities by similarity scores
            evidence_TE_prob_weighted = [
                probs * ev['score'] for probs, ev in zip(evidence_TE_prob, evidence)
                if ev['score'] > SCORE_THRESHOLD
            ]
            
            if not evidence_TE_prob_weighted:
                evidence_TE_prob_weighted = [[0, 1, 0]]
            
            # Calculate weighted sum probabilities
            claim_TE_prob_weighted_sum = np.sum(evidence_TE_prob_weighted, axis=0)
            
            # Get final labels
            claim_TE_label_weighted_sum = self.te_module.get_label_from_scores(claim_TE_prob_weighted_sum)
            claim_TE_label_malon = self.te_module.get_label_malon(evidence_TE_prob)
            
            # Store results
            te_columns['evidence_TE_prob'].append(evidence_TE_prob)
            te_columns['evidence_TE_prob_weighted'].append(evidence_TE_prob_weighted)
            te_columns['evidence_TE_labels'].append(evidence_TE_labels)
            te_columns['claim_TE_prob_weighted_sum'].append(claim_TE_prob_weighted_sum.tolist())
            te_columns['claim_TE_label_weighted_sum'].append(claim_TE_label_weighted_sum)
            te_columns['claim_TE_label_malon'].append(claim_TE_label_malon)
            te_columns['processed_timestamp'].append(datetime.now().isoformat())

        # Add results to DataFrame
        for col, values in te_columns.items():
            textual_entailment_df[col] = values

        return textual_entailment_df

    def format_results(self, evidence_df: pd.DataFrame) -> pd.DataFrame:
        """
        Format results into a readable table with all required columns
        """
        results = evidence_df.copy()
        
        all_result = pd.DataFrame()
        for idx, row in results.iterrows():
            aResult = pd.DataFrame({
                'sentence': [row['sentence']],
                'Relevance_score': [row['similarity_score']],
                'TextEntailment': [row['evidence_TE_labels'][0]],
                'Entailment_score': [max(row['evidence_TE_prob'][0])]
            })

            aBox = pd.DataFrame({
                'qid': [row.get('qid', '')],
                'property_id': [row.get('property_id', '')],
                'object_id': [row.get('object_id', '')],
                'entity_label': [row.get('entity_label', '')],
                'property_label': [row.get('property_label', '')],
                'object_label': [row.get('object_label', '')],
                'reference_id': [row.get('reference_id', '')],
                'url': [row.get('url', '')],
                'text_entailment_score': [max(row['evidence_TE_prob'][0])],
                'similarity_score': [row['similarity_score']],
                'processed_timestamp': [row.get('processed_timestamp')],
                'Results': [aResult]
            })
            
            all_result = pd.concat([all_result, aBox], axis=0)

        return all_result.reset_index(drop=True)

    def get_final_verdict(self, aggregated_result: pd.DataFrame) -> pd.DataFrame:
        """
        Get final verdict for each claim based on TextEntailment results
        """
        results = []
        for idx, row in aggregated_result.iterrows():
            temp = row.Results
            if 'SUPPORTS' in temp.TextEntailment.values:
                result_sentence = temp[temp['TextEntailment']=='SUPPORTS']['sentence'].iloc[0]
                results.append({
                    'result': 'SUPPORTS',
                    'result_sentence': result_sentence
                })
            else:
                result = temp.TextEntailment.mode()[0]
                result_sentence = temp[temp['TextEntailment']==result]['sentence'].iloc[0]
                results.append({
                    'result': result,
                    'result_sentence': result_sentence
                })
        return pd.DataFrame(results, index=aggregated_result.index)

    def process_evidence(self, sentences_df: pd.DataFrame, parser_result: Dict) -> pd.DataFrame:
        """
        Process evidence sentences and add metadata from parser_result
        Args:
            sentences_df: DataFrame containing sentences from HTML
            parser_result: Dictionary containing claim metadata
        Returns:
            DataFrame with evidence sentences and metadata
        """
        # Create copy of input DataFrame
        evidence_df = sentences_df.copy()
        
        # Add metadata from claims DataFrame
        claims_df = parser_result['claims']
        
        # Add all required metadata fields
        evidence_df['qid'] = claims_df['entity_id'].iloc[0]  # Using entity_id as qid
        evidence_df['entity_label'] = claims_df['entity_label'].iloc[0]
        evidence_df['claim_id'] = claims_df['claim_id'].iloc[0]
        evidence_df['property_id'] = claims_df['property_id'].iloc[0]
        evidence_df['property_label'] = claims_df['property_label'].iloc[0]
        evidence_df['object_id'] = claims_df['object_id'].iloc[0]
        evidence_df['object_label'] = claims_df['object_label'].iloc[0]
        
        # Add URL if exists in sentences_df
        if 'url' in sentences_df.columns:
            evidence_df['url'] = sentences_df['url']
        
        return evidence_df

    def process_entailment(self, evidence_df: pd.DataFrame, html_df: pd.DataFrame, qid: str) -> pd.DataFrame:
        """
        Main function to process entailment checking
        """
        # Add URLs from html_df using reference_id
        evidence_df = evidence_df.merge(
            html_df[['reference_id', 'url']], 
            on='reference_id', 
            how='left'
        )
        
        # Check entailment and keep original probabilities
        entailment_results = self.check_entailment(evidence_df)
        probabilities = entailment_results['evidence_TE_prob'].copy()
        
        # Format results
        aggregated_results = self.format_results(entailment_results)
        
        # Get final verdict
        final_verdict = self.get_final_verdict(aggregated_results)
        aggregated_results = pd.concat([aggregated_results, final_verdict], axis=1)
        
        # Keep only necessary columns and drop 'Results'
        final_results = aggregated_results[['text_entailment_score', 'similarity_score',
                                          'processed_timestamp', 'result',
                                          'result_sentence', 'reference_id']]
        
        # Add label probabilities using the saved probabilities
        final_results['label_probabilities'] = probabilities.apply(
            lambda x: {
                'SUPPORTS': float(x[0][0]),
                'REFUTES': float(x[0][1]),
                'NOT ENOUGH INFO': float(x[0][2])
            }
        )
        
        return final_results

if __name__ == "__main__":
    qid = 'Q44'
    
    from wikidata_parser import WikidataParser
    from refs_html_collection import HTMLFetcher
    from refs_html_to_evidences import HTMLSentenceProcessor
    from refs_html_to_evidences import EvidenceSelector

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
    
    # Check entailment with metadata
    checker = ClaimEntailmentChecker()  
    entailment_results = checker.process_entailment(evidence_df, html_df, qid)
    
    

    