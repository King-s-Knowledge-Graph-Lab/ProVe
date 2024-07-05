import pandas as pd
import numpy as np
import sqlite3, torch, json, re, os, torch, itertools
from ast import literal_eval as leval
from tqdm.auto import tqdm
from utils.verbalisation_module import VerbModule
from utils.sentence_retrieval_module import SentenceRetrievalModule
from utils.textual_entailment_module import TextualEntailmentModule
from importlib import reload
import llm_load
from html.parser import HTMLParser
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
import gradio as gr


def verbalisation(claim_df):
    verb_module = VerbModule()
    triples = []
    for _, row in claim_df.iterrows():
        triple = {
            'subject': row['entity_label'],
            'predicate': row['property_label'],
            'object': row['object_label']
        }
        triples.append(triple)


    claim_df['verbalisation'] = verb_module.verbalise_triples(triples)
    claim_df['verbalisation_unks_replaced'] = claim_df['verbalisation'].apply(verb_module.replace_unks_on_sentence)
    claim_df['verbalisation_unks_replaced_then_dropped'] = claim_df['verbalisation'].apply(lambda x: verb_module.replace_unks_on_sentence(x, empty_after=True))
    return claim_df

def RelevantSentenceSelection(verbalised_claims_df_final, reference_text_df, update_progress):
    join_df = pd.merge(verbalised_claims_df_final, reference_text_df[['reference_id', 'url', 'html']], on='reference_id', how='left')
    tokenizer, model = llm_load.llmLoad(8192)

    class BodyExtractor(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_body = False
            self.in_script = False
            self.in_iframe = False
            self.body_content = []

        def handle_starttag(self, tag, attrs):
            if tag == 'body':
                self.in_body = True
            elif tag == 'script':
                self.in_script = True
            elif tag == 'iframe':
                self.in_iframe = True

        def handle_endtag(self, tag):
            if tag == 'body':
                self.in_body = False
            elif tag == 'script':
                self.in_script = False
            elif tag == 'iframe':
                self.in_iframe = False

        def handle_data(self, data):
            if self.in_body and not self.in_script and not self.in_iframe:
                self.body_content.append(data)

        def get_body_content(self):
            return ''.join(self.body_content)

    def extract_body(html):
        parser = BodyExtractor()
        parser.feed(html)
        return parser.get_body_content()

    filtered_htmls = []
    answers = []
    verifications = []
    for idx, (html, verb) in enumerate(zip(join_df['html'], join_df['verbalisation'])):
        try:
            filtered_html = extract_body(html)
            filtered_htmls.append(filtered_html)
            instruct = "Find the most relevant sentences from the filtered HTML document based on the given target sentence. If there are no directly related sentences, try to find sentences that provide context or background information related to the target sentence. Only answer 'nothing' if there is absolutely no relevant information in the document. Do not include any HTML tags or markup in your answer."
            question = f"target sentence:'{verb}', filtered HTML dcoument:{filtered_html}"
            answer = llm_load.llmQuestion(tokenizer, model, instruct, question, output_size=128)
            answers.append(answer)
        except:
            answers.append('Malformed html')
        instruct = "Determine whether the target sentence is supported by the given evidence or not. If so, answer 'supportive'. It not, answer 'No supports'. Or, you can't determine with the given evidence, then asnwer 'Not enough information'"
        question = f"target sentence:'{verb}', evidence:{answers[-1]}"
        verification = llm_load.llmQuestion(tokenizer, model, instruct, question, output_size=64)
        verifications.append(verification)
        
        update_progress(idx, len(join_df))  # Update progress

    return pd.DataFrame({'verbalisation': join_df['verbalisation'], 'verification': verifications, 'evidence_set': answers, 'url': join_df['url'], 'filtered_html': filtered_htmls})



if __name__ == '__main__':
    target_QID = 'Q42'
    conn = sqlite3.connect('wikidata_claims_refs_parsed.db')
    query = f"SELECT * FROM claim_text WHERE entity_id = '{target_QID}'"
    claim_df = pd.read_sql_query(query, conn)
    
    query = f"SELECT * FROM html_text Where  entity_id = '{target_QID}'"
    reference_text_df = pd.read_sql_query(query, conn)
    
    verbalised_claims_df_final = verbalisation(claim_df)

    progress = gr.Progress(len(verbalised_claims_df_final))  # Create progress bar
    def update_progress(curr_step, total_steps):
        progress((curr_step + 1) / total_steps)

    result = RelevantSentenceSelection(tokenizer, model, verbalised_claims_df_final, reference_text_df, update_progress)

    conn.commit()
    conn.close()