from wikidata_parser import WikidataParser
from refs_html_collection import HTMLFetcher
from refs_html_to_evidences import HTMLSentenceProcessor
from refs_html_to_evidences import EvidenceSelector
from claim_entailment import process_entailment

if __name__ == "__main__":
    qid = 'Q44'
    
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
    entailment_results = process_entailment(evidence_df, html_df, qid)
    