import pandas as pd
import numpy as np
import sqlite3, torch, json, re, os, torch, itertools, nltk
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
from bs4 import BeautifulSoup
from cleantext import clean


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

def setencesSpliter(verbalised_claims_df_final, reference_text_df, update_progress):
    join_df = pd.merge(verbalised_claims_df_final, reference_text_df[['reference_id', 'url', 'html']], on='reference_id', how='left')
    SS_df = join_df[['reference_id','url','verbalisation', 'html']].copy()
    def clean_html(html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)  
        cleaned_text = clean(text,
                            fix_unicode=True,  
                            to_ascii=True, 
                            lower=False,  
                            no_line_breaks=False,  
                            no_urls=True, 
                            no_emails=True,  
                            no_phone_numbers=True, 
                            no_numbers=False,  
                            no_digits=False, 
                            no_currency_symbols=True,  
                            no_punct=False, 
                            replace_with_url="",
                            replace_with_email="",
                            replace_with_phone_number="",
                            replace_with_number="",
                            replace_with_digit="",
                            replace_with_currency_symbol="")
        return cleaned_text
    def split_into_sentences(text):
        sentences = nltk.sent_tokenize(text)
        return sentences
    def slide_sentences(sentences, window_size=2):
        if len(sentences) < window_size:
            return [" ".join(sentences)]
        return [" ".join(sentences[i:i + window_size]) for i in range(len(sentences) - window_size + 1)]
    
    SS_df['html2text'] = SS_df['html'].apply(clean_html)
    SS_df['nlp_sentences'] = SS_df['html2text'].apply(split_into_sentences)
    SS_df['nlp_sentences_slide_2'] = SS_df['nlp_sentences'].apply(slide_sentences)

    return SS_df[['reference_id','verbalisation','url','nlp_sentences','nlp_sentences_slide_2']]

def evidenceSelection(splited_sentences_from_html, BATCH_SIZE, N_TOP_SENTENCES):
    sr_module = SentenceRetrievalModule(max_len=512)
    sentence_relevance_df = splited_sentences_from_html.copy()
    sentence_relevance_df.rename(columns={'verbalisation': 'final_verbalisation'}, inplace=True)

    def chunks(l, n):
        n = max(1, n)
        return [l[i:i + n] for i in range(0, len(l), n)]
    
    def compute_scores(column_name):
        all_outputs = []
        for _, row in tqdm(sentence_relevance_df.iterrows(), total=sentence_relevance_df.shape[0]):
            outputs = []
            for batch in chunks(row[column_name], BATCH_SIZE):
                batch_outputs = sr_module.score_sentence_pairs([(row['final_verbalisation'], sentence) for sentence in batch])
                outputs += batch_outputs
            all_outputs.append(outputs)
        sentence_relevance_df[f'{column_name}_scores'] = pd.Series(all_outputs)
        assert all(sentence_relevance_df.apply(lambda x: len(x[column_name]) == len(x[f'{column_name}_scores']), axis=1))

    compute_scores('nlp_sentences')
    compute_scores('nlp_sentences_slide_2')

    def get_top_n_sentences(row, column_name, n):
        sentences_with_scores = [{'sentence': t[0], 'score': t[1], 'sentence_id': str(j)} for j, t in enumerate(zip(row[column_name], row[f'{column_name}_scores']))]
        return sorted(sentences_with_scores, key=lambda x: x['score'], reverse=True)[:n]
    
    def filter_overlaps(sentences):
        filtered = []
        for evidence in sentences:
            if ';' in evidence['sentence_id']:
                start_id, end_id = evidence['sentence_id'].split(';')
                if not any(start_id in e['sentence_id'].split(';') or end_id in e['sentence_id'].split(';') for e in filtered):
                    filtered.append(evidence)
            else:
                if not any(evidence['sentence_id'] in e['sentence_id'].split(';') for e in filtered):
                    filtered.append(evidence)
        return filtered

    nlp_sentences_TOP_N, nlp_sentences_slide_2_TOP_N, nlp_sentences_all_TOP_N = [], [], []
    
    for _, row in tqdm(sentence_relevance_df.iterrows(), total=sentence_relevance_df.shape[0]):
        top_n = get_top_n_sentences(row, 'nlp_sentences', N_TOP_SENTENCES)
        nlp_sentences_TOP_N.append(top_n)
        
        top_n_slide_2 = get_top_n_sentences(row, 'nlp_sentences_slide_2', N_TOP_SENTENCES)
        nlp_sentences_slide_2_TOP_N.append(top_n_slide_2)
        
        all_sentences = top_n + top_n_slide_2
        all_sentences_sorted = sorted(all_sentences, key=lambda x: x['score'], reverse=True)
        filtered_sentences = filter_overlaps(all_sentences_sorted)
        nlp_sentences_all_TOP_N.append(filtered_sentences[:N_TOP_SENTENCES])
    
    sentence_relevance_df['nlp_sentences_TOP_N'] = pd.Series(nlp_sentences_TOP_N)
    sentence_relevance_df['nlp_sentences_slide_2_TOP_N'] = pd.Series(nlp_sentences_slide_2_TOP_N)
    sentence_relevance_df['nlp_sentences_all_TOP_N'] = pd.Series(nlp_sentences_all_TOP_N)
    
    return sentence_relevance_df

def textEntailment(evidence_df, SCORE_THRESHOLD):
    textual_entailment_df = evidence_df.copy()
    te_module = TextualEntailmentModule()

    keys = ['TOP_N', 'slide_2_TOP_N', 'all_TOP_N']
    te_columns = {f'evidence_TE_prob_{key}': [] for key in keys}
    te_columns.update({f'evidence_TE_prob_weighted_{key}': [] for key in keys})
    te_columns.update({f'evidence_TE_labels_{key}': [] for key in keys})
    te_columns.update({f'claim_TE_prob_weighted_sum_{key}': [] for key in keys})
    te_columns.update({f'claim_TE_label_weighted_sum_{key}': [] for key in keys})
    te_columns.update({f'claim_TE_label_malon_{key}': [] for key in keys})

    def process_row(row):
        claim = row['final_verbalisation']
        results = {}
        for key in keys:
            evidence = row[f'nlp_sentences_{key}']
            evidence_size = len(evidence)
            if evidence_size == 0:
                results[key] = {
                    'evidence_TE_prob': [],
                    'evidence_TE_labels': [],
                    'evidence_TE_prob_weighted': [],
                    'claim_TE_prob_weighted_sum': [0, 0, 0],
                    'claim_TE_label_weighted_sum': 'NOT ENOUGH INFO',
                    'claim_TE_label_malon': 'NOT ENOUGH INFO'
                }
                continue

            evidence_TE_prob = te_module.get_batch_scores(
                claims=[claim] * evidence_size,
                evidence=[e['sentence'] for e in evidence]
            )

            evidence_TE_labels = [te_module.get_label_from_scores(s) for s in evidence_TE_prob]

            evidence_TE_prob_weighted = [
                probs * ev['score'] for probs, ev in zip(evidence_TE_prob, evidence)
                if ev['score'] > SCORE_THRESHOLD
            ]

            claim_TE_prob_weighted_sum = np.sum(evidence_TE_prob_weighted, axis=0) if evidence_TE_prob_weighted else [0, 0, 0]

            claim_TE_label_weighted_sum = te_module.get_label_from_scores(claim_TE_prob_weighted_sum) if evidence_TE_prob_weighted else 'NOT ENOUGH INFO'

            claim_TE_label_malon = te_module.get_label_malon(
                [probs for probs, ev in zip(evidence_TE_prob, evidence) if ev['score'] > SCORE_THRESHOLD]
            )

            results[key] = {
                'evidence_TE_prob': evidence_TE_prob,
                'evidence_TE_labels': evidence_TE_labels,
                'evidence_TE_prob_weighted': evidence_TE_prob_weighted,
                'claim_TE_prob_weighted_sum': claim_TE_prob_weighted_sum,
                'claim_TE_label_weighted_sum': claim_TE_label_weighted_sum,
                'claim_TE_label_malon': claim_TE_label_malon
            }
        return results

    for i, row in tqdm(textual_entailment_df.iterrows(), total=textual_entailment_df.shape[0]):
        try:
            result_sets = process_row(row)
            for key in keys:
                for k, v in result_sets[key].items():
                    te_columns[f'{k}_{key}'].append(v)
        except Exception as e:
            print(f"Error processing row {i}: {e}")
            print(row)
            raise

    for key in keys:
        for col in ['evidence_TE_prob', 'evidence_TE_prob_weighted', 'evidence_TE_labels',
                    'claim_TE_prob_weighted_sum', 'claim_TE_label_weighted_sum', 'claim_TE_label_malon']:
            textual_entailment_df[f'{col}_{key}'] = pd.Series(te_columns[f'{col}_{key}'])

    return textual_entailment_df

if __name__ == '__main__':
    target_QID = 'Q42'
    conn = sqlite3.connect('wikidata_claims_refs_parsed.db')
    query = f"SELECT * FROM claim_text WHERE entity_id = '{target_QID}'"
    claim_df = pd.read_sql_query(query, conn)
    
    query = f"SELECT * FROM html_text Where  entity_id = '{target_QID}'"
    reference_text_df = pd.read_sql_query(query, conn)
    
    verbalised_claims_df_final = verbalisation(claim_df)

    progress = gr.Progress(len(verbalised_claims_df_final))  # Create progress bar for Gradio
    def update_progress(curr_step, total_steps):
        progress((curr_step + 1) / total_steps)

    splited_sentences_from_html = setencesSpliter(verbalised_claims_df_final, reference_text_df, update_progress)

    BATCH_SIZE = 512
    N_TOP_SENTENCES = 5
    SCORE_THRESHOLD = 0
    evidence_df = evidenceSelection(splited_sentences_from_html, BATCH_SIZE, N_TOP_SENTENCES)
    result = textEntailment(evidence_df, SCORE_THRESHOLD)

    conn.commit()
    conn.close()