import pandas as pd
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from functions import *



#Results extraction for pagePile list on the specific date
def pagePile_results_extraction(processed_date):
    q_list = pd.read_csv('CodeArchive/pagepile.csv', header=None)[0]
    db_path = 'reference_checked.db'
    table_name = 'original_results'
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
        
def analyze_pagepile_processing():
    """
    Analyzes the processing status of Pagepile QIDs from the status table.
    Returns processing history and statistics for pagepile items.
    """
    # Get QID list from Pagepile
    pagepile_df = pd.read_csv('CodeArchive/pagepile.csv', header=None, names=['qid', 'description'])
    pagepile_qids = pagepile_df['qid'].tolist()
    
    # Query status data from DB
    db_path = 'reference_checked.db'
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT s.task_id, s.qid, s.status, s.start_time, s.algo_version, s.request_type
    FROM status s
    WHERE s.qid IN ({})
    ORDER BY s.start_time DESC
    """.format(','.join('?' * len(pagepile_qids)))
    
    status_df = pd.read_sql_query(query, conn, params=pagepile_qids)
    conn.close()
    
    if not status_df.empty:
        # Convert start_time to datetime
        status_df['start_time'] = pd.to_datetime(status_df['start_time'])
        status_df['date'] = status_df['start_time'].dt.date
        
        # Daily processing counts
        daily_counts = status_df.groupby(['date', 'status']).size().unstack(fill_value=0)
        daily_counts = daily_counts.sort_index(ascending=False)  # Most recent dates first
        
        print("\n=== Pagepile Processing Analysis ===")
        print("\nDaily Processing Status:")
        print(daily_counts)
        
        # Status distribution
        print("\nOverall Status Distribution:")
        print(status_df['status'].value_counts())
        
        # Latest processing status for each QID
        latest_status = status_df.sort_values('start_time').groupby('qid').last()
        print("\nLatest Status for Each QID:")
        print(latest_status[['status', 'start_time', 'algo_version']].head())
        
        return {
            'daily_counts': daily_counts,
            'status_distribution': status_df['status'].value_counts(),
            'latest_status': latest_status,
            'raw_data': status_df
        }
    else:
        print("No processing records found for Pagepile QIDs")
        return None

if __name__ == "__main__":
    processed_date = "2024-09-06"
    #pagePile_results_extraction(processed_date)
    
    # Add Pagepile processing analysis
    analysis = analyze_pagepile_processing()
    