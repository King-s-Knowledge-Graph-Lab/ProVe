from wikidata_parser import WikidataParser
from refs_html_collection import HTMLFetcher
from refs_html_to_evidences import HTMLSentenceProcessor, EvidenceSelector
from claim_entailment import ClaimEntailmentChecker
from utils.textual_entailment_module import TextualEntailmentModule
from utils.sentence_retrieval_module import SentenceRetrievalModule
from utils.verbalisation_module import VerbModule

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
    
    # Initialize processors with pre-loaded models
    selector = EvidenceSelector(sentence_retrieval=sentence_retrieval, 
                              verb_module=verb_module)
    checker = ClaimEntailmentChecker(text_entailment=text_entailment)
    
    # Get URLs and claims
    parser = WikidataParser()
    parser_result = parser.process_entity(qid)
    parser_stats = parser.get_processing_stats()
    # Fetch HTML content
    fetcher = HTMLFetcher(config_path='config.yaml')
    html_df = fetcher.fetch_all_html(parser_result['urls'], parser_result)
    
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
    qid = 'Q245247'
    html_df, entailment_results, parser_stats = process_entity(qid, models)
    
    
    