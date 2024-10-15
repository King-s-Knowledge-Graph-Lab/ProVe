import pandas as pd
import sqlite3
from functions import *

#Periodically processing for pagePile list
pagepile_df = pd.read_csv('CodeArchive/pagepile.csv', header=None)
prior_item_list_df = pd.read_csv('CodeArchive/prior_item_list.csv')
prior_item_list_df['qid'] = pagepile_df[0]

prior_item_list_df.to_csv('CodeArchive/prior_item_list.csv', index=False)

print("prior_item_list.csv has been successfully updated.")

#Results extraction for pagePile list on the specific date
q_list = pd.read_csv('CodeArchive/pagepile.csv', header=None)[0]
db_path = 'reference_checked.db'
table_name = 'original_results'
processed_date = "2024-09-06"
columns = [
    'id', 'final_verbalisation', 'url', 'nlp_sentences', 'nlp_sentences_slide_2',
    'nlp_sentences_scores', 'nlp_sentences_slide_2_scores', 'nlp_sentences_TOP_N',
    'nlp_sentences_slide_2_TOP_N', 'nlp_sentences_all_TOP_N', 'evidence_TE_prob_TOP_N',
    'evidence_TE_prob_weighted_TOP_N', 'evidence_TE_labels_TOP_N', 'claim_TE_prob_weighted_sum_TOP_N',
    'claim_TE_label_weighted_sum_TOP_N', 'claim_TE_label_malon_TOP_N', 'evidence_TE_prob_slide_2_TOP_N',
    'evidence_TE_prob_weighted_slide_2_TOP_N', 'evidence_TE_labels_slide_2_TOP_N',
    'claim_TE_prob_weighted_sum_slide_2_TOP_N', 'claim_TE_label_weighted_sum_slide_2_TOP_N',
    'claim_TE_label_malon_slide_2_TOP_N', 'evidence_TE_prob_all_TOP_N', 'evidence_TE_prob_weighted_all_TOP_N',
    'evidence_TE_labels_all_TOP_N', 'claim_TE_prob_weighted_sum_all_TOP_N', 'claim_TE_label_weighted_sum_all_TOP_N',
    'claim_TE_label_malon_all_TOP_N', 'qid', 'processed_timestamp', 'task_id'
]


def qitemReading(qid, processed_date):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = '''
    SELECT * FROM original_results
    WHERE qid = ? AND processed_timestamp LIKE ?
    '''

    cursor.execute(query, (qid, processed_date + '%'))
    results = cursor.fetchall()

    conn.close()

    return results

def read_sqlite_table_to_dataframe(db_path, table_name, qid_list):
    try:
        conn = sqlite3.connect(db_path)
        query = f"""
        SELECT t1.*
        FROM {table_name} t1
        INNER JOIN (
            SELECT qid, MAX(id) as max_id
            FROM {table_name}
            WHERE qid IN ({','.join(['?']*len(qid_list))})
            GROUP BY qid
        ) t2 ON t1.qid = t2.qid AND t1.id = t2.max_id
        """
        df = pd.read_sql_query(query, conn, params=qid_list)
        conn.close()
        print(f"Successfully retrieved the latest QID data from the '{table_name}' table.")
        return df
    except sqlite3.Error as e:
        print(f"SQLite error occurred: {e}")
        return None
    except pd.io.sql.DatabaseError as e:
        print(f"pandas database error occurred: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        return None

all_results = []

for q in q_list:
    all_results.extend(qitemReading(q, processed_date))  

df = pd.DataFrame(all_results, columns=columns)
selected_columns = [
    'id', 'final_verbalisation', 'url', 'nlp_sentences', 'nlp_sentences_scores',
    'nlp_sentences_TOP_N', 'evidence_TE_prob_TOP_N', 'claim_TE_label_weighted_sum_TOP_N',
    'qid', 'task_id', 'processed_timestamp'
]

df_selected = df[selected_columns]
#Output 1
df_selected.to_excel(f'CodeArchive/pagePile{processed_date}.xlsx', index=False)


list_input = df_selected.copy()
qid_list_to_check = list_input['qid'].tolist()


df = read_sqlite_table_to_dataframe(db_path, table_name, qid_list_to_check)
if df is not None:
    selected_columns = ['id', 'final_verbalisation', 'url', 'nlp_sentences', 'nlp_sentences_scores', 'nlp_sentences_TOP_N', 'evidence_TE_prob_TOP_N', 'claim_TE_label_weighted_sum_TOP_N', 'qid', 'task_id', 'processed_timestamp'
                        ] 
    df_selected = df[selected_columns]
    df_selected['comprehensive_results'] = df_selected['qid'].apply(lambda x: comprehensive_results(x))
    #Output 2
    df_selected = df_selected[['qid', 'processed_timestamp', 'comprehensive_results']]
    df_selected.to_excel(f'CodeArchive/pagePile{processed_date}_with_score.xlsx', index=False)
else:
    print("Failed to retrieve data.")