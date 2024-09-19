import pandas as pd
import sqlite3

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
        print(f"'{table_name}' 테이블에서 최신 QID 데이터를 성공적으로 가져왔습니다.")
        return df
    except sqlite3.Error as e:
        print(f"SQLite 오류 발생: {e}")
        return None
    except pd.io.sql.DatabaseError as e:
        print(f"pandas 데이터베이스 오류 발생: {e}")
        return None
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
        return None



list = pd.read_csv('CodeArchive/pagepile.csv', header=None)
db_path = 'reference_checked.db'
table_name = 'original_results'
qid_list_to_check = list[0].tolist()

df = read_sqlite_table_to_dataframe(db_path, table_name, qid_list_to_check)
if df is not None:
    selected_columns = ['id', 'final_verbalisation', 'url', 'nlp_sentences', 'nlp_sentences_scores', 'nlp_sentences_TOP_N', 'evidence_TE_prob_TOP_N', 'claim_TE_label_weighted_sum_TOP_N', 'qid', 'task_id', 'processed_timestamp'
                        ] 
    df_selected = df[selected_columns]
    df_selected.to_excel('CodeArchive/resultPagepile.xlsx', index=False)
else:
    print("데이터를 가져오는 데 실패했습니다.")