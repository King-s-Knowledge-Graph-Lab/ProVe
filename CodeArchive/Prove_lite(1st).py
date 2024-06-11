import pandas as pd
import numpy as np
import sqlite3, torch, json, re
from ast import literal_eval as leval
from tqdm.auto import tqdm
from utils.verbalisation_module import VerbModule
from utils.sentence_retrieval_module import SentenceRetrievalModule
from utils.textual_entailment_module import TextualEntailmentModule
from importlib import reload


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

def RelevantSentenceSelection(verbalised_claims_df_final, reference_text_df, BATCH_SIZE, N_TOP_SENTENCES):

    verbalised_claims_df_final['final_verbalisation'] = verbalised_claims_df_final['verbalisation'].copy()


    sentence_relevance_df = pd.merge(
        verbalised_claims_df_final,
        reference_text_df,
        how='left',
        on='reference_id'
    )
    sentence_relevance_df['nlp_sentences'] = sentence_relevance_df['nlp_sentences'].apply(leval)
    sentence_relevance_df['nlp_sentences_slide_2'] = sentence_relevance_df['nlp_sentences_slide_2'].apply(leval)
    sr_module = SentenceRetrievalModule(max_len=512)
    sentence_relevance_df['nlp_sentences_scores'] = None
    sentence_relevance_df['nlp_sentences_slide_2_scores'] = None

    def chunks(l, n):
        n = max(1, n)
        return [l[i:i+n] for i in range(0, len(l), n)]

    all_outputs = []
    for i, row in tqdm(sentence_relevance_df.iterrows(), total=sentence_relevance_df.shape[0]):
        
        outputs = []
        for batch in chunks(row['nlp_sentences'], BATCH_SIZE):
            batch_outputs = sr_module.score_sentence_pairs(
                [(row['final_verbalisation'], sentence) for sentence in batch]
            )
            outputs += batch_outputs
        all_outputs.append(outputs)
        
    all_outputs = pd.Series(all_outputs)
    sentence_relevance_df['nlp_sentences_scores'] = all_outputs

    assert all(sentence_relevance_df.apply(
        lambda x : len(x['nlp_sentences']) == len(x['nlp_sentences_scores']),
        axis=1
    ))

    all_outputs = []
    for i, row in tqdm(sentence_relevance_df.iterrows(), total=sentence_relevance_df.shape[0]):
        
        outputs = []
        for batch in chunks(row['nlp_sentences_slide_2'], BATCH_SIZE):
            batch_outputs = sr_module.score_sentence_pairs(
                [(row['final_verbalisation'], sentence) for sentence in batch]
            )
            outputs += batch_outputs
        all_outputs.append(outputs)
        
    all_outputs = pd.Series(all_outputs)    
    sentence_relevance_df['nlp_sentences_slide_2_scores'] = all_outputs
        

    assert all(sentence_relevance_df.apply(
        lambda x : len(x['nlp_sentences_slide_2']) == len(x['nlp_sentences_slide_2_scores']),
        axis=1
    ))

    nlp_sentences_TOP_N = []
    nlp_sentences_slide_2_TOP_N = []
    nlp_sentences_all_TOP_N = []

    for i, row in tqdm(sentence_relevance_df.iterrows(), total=sentence_relevance_df.shape[0]):

        nlp_sentences_with_scores = [{
            'sentence': t[0],
            'score': t[1],
            'sentence_id': str(j)
        } for j, t in enumerate(zip(row['nlp_sentences'], row['nlp_sentences_scores']))]

        nlp_sentences_with_scores = sorted(nlp_sentences_with_scores, key = lambda x : x['score'], reverse = True)
        nlp_sentences_TOP_N.append(nlp_sentences_with_scores[:N_TOP_SENTENCES])
        
        nlp_sentences_slide_2_with_scores = [{
            'sentence': t[0],
            'score': t[1],
            'sentence_id': str(j)+';'+str(j+1)
        } for j, t in enumerate(zip(row['nlp_sentences_slide_2'], row['nlp_sentences_slide_2_scores']))]

        nlp_sentences_slide_2_with_scores = sorted(nlp_sentences_slide_2_with_scores, key = lambda x : x['score'], reverse = True)
        nlp_sentences_slide_2_TOP_N.append(nlp_sentences_slide_2_with_scores[:N_TOP_SENTENCES])
        

        nlp_sentences_all_with_scores = nlp_sentences_with_scores + nlp_sentences_slide_2_with_scores
        nlp_sentences_all_with_scores = sorted(nlp_sentences_all_with_scores, key = lambda x : x['score'], reverse = True)
        
        #We might no want to allow overlaps, so we do the following:
        #For each evidence in descending order of score, we delete from the 'all' list
        #all overlapping evidence scored lower than it
        nlp_sentences_all_with_scores_filtered_for_overlap = []
        for evidence in nlp_sentences_all_with_scores:
            if ';' in evidence['sentence_id']:
                [start_id, end_id] = evidence['sentence_id'].split(';')
                if not any(
                    [start_id in e['sentence_id'].split(';') for e in nlp_sentences_all_with_scores_filtered_for_overlap]
                ):
                    if not any(
                        [end_id in e['sentence_id'].split(';') for e in nlp_sentences_all_with_scores_filtered_for_overlap]
                    ):
                        nlp_sentences_all_with_scores_filtered_for_overlap.append(evidence)
            else:
                if not any(
                    [evidence['sentence_id'] in e['sentence_id'].split(';') for e in nlp_sentences_all_with_scores_filtered_for_overlap]
                ):
                    nlp_sentences_all_with_scores_filtered_for_overlap.append(evidence)
        
        
        if len(nlp_sentences_all_with_scores_filtered_for_overlap) >= N_TOP_SENTENCES:
            nlp_sentences_all_TOP_N.append(nlp_sentences_all_with_scores_filtered_for_overlap[:N_TOP_SENTENCES])
        else:
            nlp_sentences_all_TOP_N.append(nlp_sentences_all_with_scores_filtered_for_overlap)  
        nlp_sentences_all_TOP_N.append(nlp_sentences_all_with_scores_filtered_for_overlap[:N_TOP_SENTENCES])
        
    sentence_relevance_df['nlp_sentences_TOP_N'] = pd.Series(nlp_sentences_TOP_N)
    sentence_relevance_df['nlp_sentences_slide_2_TOP_N'] = pd.Series(nlp_sentences_slide_2_TOP_N)
    sentence_relevance_df['nlp_sentences_all_TOP_N'] = pd.Series(nlp_sentences_all_TOP_N)

    sentence_relevance_df.iloc[1].nlp_sentences_all_TOP_N
    return sentence_relevance_df

def textEntailment(sentence_relevance_df, SCORE_THRESHOLD):
    te_module = TextualEntailmentModule()
    textual_entailment_df = sentence_relevance_df.copy()

    keys = ['TOP_N', 'slide_2_TOP_N', 'all_TOP_N']
    te_columns = {}

    for key in keys:
        te_columns[f'evidence_TE_prob_{key}'] = []
        te_columns[f'evidence_TE_prob_weighted_{key}'] = []
        te_columns[f'evidence_TE_labels_{key}'] = []
        te_columns[f'claim_TE_prob_weighted_sum_{key}'] = []
        te_columns[f'claim_TE_label_weighted_sum_{key}'] = []
        te_columns[f'claim_TE_label_malon_{key}'] = []


    for i, row in tqdm(textual_entailment_df.iterrows(), total=textual_entailment_df.shape[0]):
        try:
            claim = row['final_verbalisation']

            result_sets = {key : {'evidence': row[f'nlp_sentences_{key}']} for key in keys}

            for key, rs in result_sets.items():

                evidence_size = len([e for e in rs['evidence']])
            
                rs['evidence_TE_prob'] = te_module.get_batch_scores(
                    claims = [claim for _ in range(evidence_size)],
                    evidence = [e['sentence'] for e in rs['evidence']]
                )   
                
                rs['evidence_TE_labels'] = [te_module.get_label_from_scores(s) for s in rs['evidence_TE_prob']]
                    
                rs['evidence_TE_prob_weighted'] = [
                    probs*ev['score'] for probs, ev in zip(rs['evidence_TE_prob'], rs['evidence'])\
                    if ev['score'] > SCORE_THRESHOLD
                ]
                
                rs['claim_TE_prob_weighted_sum'] = \
                    np.sum(rs['evidence_TE_prob_weighted'], axis=0)\
                    if rs['evidence_TE_prob_weighted'] else [0,0,0]
                
                rs['claim_TE_label_weighted_sum'] = \
                    te_module.get_label_from_scores(rs['claim_TE_prob_weighted_sum'])\
                    if rs['evidence_TE_prob_weighted'] else 'NOT ENOUGH INFO'  
                

                rs['claim_TE_label_malon'] = te_module.get_label_malon(
                    probs for probs, ev in zip(rs['evidence_TE_prob'], rs['evidence'])\
                    if ev['score'] > SCORE_THRESHOLD
                )

                te_columns[f'evidence_TE_prob_{key}'].append(rs['evidence_TE_prob'])
                te_columns[f'evidence_TE_prob_weighted_{key}'].append(rs['evidence_TE_prob_weighted'])
                te_columns[f'evidence_TE_labels_{key}'].append(rs['evidence_TE_labels'])
                te_columns[f'claim_TE_prob_weighted_sum_{key}'].append(rs['claim_TE_prob_weighted_sum'])
                te_columns[f'claim_TE_label_weighted_sum_{key}'].append(rs['claim_TE_label_weighted_sum'])
                te_columns[f'claim_TE_label_malon_{key}'].append(rs['claim_TE_label_malon'])
                
                #print(rs)
                #break
        
        except Exception:
            print(row)
            print(result_sets)

            raise
        
        #break

    for key in keys:
        textual_entailment_df[f'evidence_TE_prob_{key}'] = pd.Series(te_columns[f'evidence_TE_prob_{key}'])
        textual_entailment_df[f'evidence_TE_prob_weighted_{key}'] = pd.Series(te_columns[f'evidence_TE_prob_weighted_{key}'])
        textual_entailment_df[f'evidence_TE_labels_{key}'] = pd.Series(te_columns[f'evidence_TE_labels_{key}'])
        textual_entailment_df[f'claim_TE_prob_weighted_sum_{key}'] = pd.Series(te_columns[f'claim_TE_prob_weighted_sum_{key}'])
        textual_entailment_df[f'claim_TE_label_weighted_sum_{key}'] = pd.Series(te_columns[f'claim_TE_label_weighted_sum_{key}'])
        textual_entailment_df[f'claim_TE_label_malon_{key}'] = pd.Series(te_columns[f'claim_TE_label_malon_{key}'])

    return textual_entailment_df

if __name__ == '__main__':
    target_QID = 'Q42'
    conn = sqlite3.connect('wikidata_claims_refs_parsed.db')
    query = f"SELECT * FROM claim_text WHERE entity_id = '{target_QID}'"
    claim_df = pd.read_sql_query(query, conn)
    
    query = f"SELECT * FROM html_text"
    reference_text_df = pd.read_sql_query(query, conn)
    
    BATCH_SIZE = 1024
    N_TOP_SENTENCES = 5
    SCORE_THRESHOLD = 0
    TEST_SIZE = 5 #Sampling for Easy Test

    verbalised_claims_df_final = verbalisation(claim_df)
    #verbalised_claims_df_final = verbalised_claims_df_final.sample(n=TEST_SIZE, replace=False) #data sampling for easy-test
    sentence_relevance_df = RelevantSentenceSelection(verbalised_claims_df_final, reference_text_df, BATCH_SIZE, N_TOP_SENTENCES)
    result = textEntailment(sentence_relevance_df, SCORE_THRESHOLD)
    result.to_csv('results.csv', index=False)


    conn.commit()
    conn.close()