from wikidata_parser import WikidataParser
from refs_html_collection import HTMLFetcher
from .LLM_refs_html_to_evidences import HTMLSentenceProcessor, EvidenceSelector
from claim_entailment import ClaimEntailmentChecker
from utils.textual_entailment_module import TextualEntailmentModule
from utils.sentence_retrieval_module import SentenceRetrievalModule
from utils.verbalisation_module import VerbModule
import pandas as pd

def initialize_models():
    """Initialize all required models once"""
    text_entailment = TextualEntailmentModule()
    sentence_retrieval = SentenceRetrievalModule(max_len=512)
    verb_module = VerbModule()
    return text_entailment, sentence_retrieval, verb_module


def process_entity(qid: str, models: tuple) -> tuple:
    """
    Process a single entity with pre-loaded models
    """
    text_entailment, sentence_retrieval, verb_module = models
    
    # Get URLs and claims
    parser = WikidataParser()
    parser_result = parser.process_entity(qid)
    parser_stats = parser.get_processing_stats()
    
    # Check if URLs exist
    if 'urls' not in parser_result or parser_result['urls'].empty:
        # Return empty DataFrames and parser stats
        empty_df = pd.DataFrame()
        empty_results = pd.DataFrame()
        return empty_df, empty_results, parser_stats
    
    # Initialize processors with pre-loaded models
    selector = EvidenceSelector(sentence_retrieval=sentence_retrieval, 
                              verb_module=verb_module)
    checker = ClaimEntailmentChecker(text_entailment=text_entailment)
    
    # Fetch HTML content
    fetcher = HTMLFetcher(config_path='config.yaml')
    html_df = fetcher.fetch_all_html(parser_result['urls'], parser_result)
    
    # Check if there are any successful (status 200) URLs
    if not (html_df['status'] == 200).any():
        # Return current html_df with failed fetches, empty results and parser stats
        empty_results = pd.DataFrame()
        return html_df, empty_results, parser_stats
    
    # Convert HTML to sentences
    processor = HTMLSentenceProcessor()
    sentences_df = processor.process_html_to_sentences(html_df)
    
    # Process evidence selection
    evidence_df = selector.process_evidence(sentences_df, parser_result)
    
    # Check entailment with metadata
    entailment_results = checker.process_entailment(evidence_df, html_df, qid)
    
    return html_df, entailment_results, parser_stats

if __name__ == "__main__":
    # Initialize models once
    models = initialize_models()
    
    # Process entity
    qid = 'Q44'
    html_df, entailment_results, parser_stats = process_entity(qid, models)
    
    
    
