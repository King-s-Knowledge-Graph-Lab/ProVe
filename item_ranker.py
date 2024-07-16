import sqlite3
import pandas as pd
from tqdm import tqdm

#Params.
db_path = 'reference_checked.db'

#Utils.
def read_aggregated_results(db_path):
    conn = sqlite3.connect(db_path)
    query = "SELECT * FROM aggregated_results"
    df = pd.read_sql_query(query, conn, index_col='id')
    conn.close()
    return df

def get_filtered_data(db_path, table_name, column_name, filter_value):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    
    query = f"SELECT * FROM {table_name} WHERE {column_name} = ?"
    cursor.execute(query, (filter_value,))
    results = cursor.fetchall()
    
    conn.close()
    
    data = [dict(zip(columns, row)) for row in results]
    return data


readings = read_aggregated_results(db_path)

qid_list = readings['qid'].unique()
all_categories = ['NOT ENOUGH INFO', 'SUPPORTS', 'REFUTES']

##1. Item ranking
ranks = []
for qid in tqdm(qid_list):
    subset = readings[readings['qid']==qid]
    subset = subset[subset['task_id']==subset['task_id'].iloc[-1]] #selecting only recent task one
    value_counts = subset['result'].value_counts(dropna=False).reindex(all_categories, fill_value=0)
    tot_num = value_counts.sum()
    abs_num_REF = value_counts.get('REFUTES')
    abs_num_NEI = value_counts.get('NOT ENOUGH INFO')
    ranks.append({'qid': qid, 'abs_REFUTES': abs_num_REF, 'abs_num_NEI': abs_num_NEI,'ratio_REFUTES': abs_num_REF/tot_num, 'ratio_NEI': abs_num_NEI/tot_num})

ranks = pd.DataFrame(ranks)
top_n = ranks.sort_values(by=['abs_REFUTES'], ascending=False).iloc[:30]
data = pd.DataFrame(get_filtered_data(db_path, 'aggregated_results', 'qid', f'Q517'))
data = data[data['result']=='REFUTES']
data[['triple','result', 'url']]

##2. Property (type) ranking
readings[['Subject', 'Predicate', 'Object']] = readings['triple'].str.split(',', expand=True, n=2)
predicate_list = readings['Predicate'].unique()
ranks = []
for qid in tqdm(predicate_list):
    subset = readings[readings['Predicate']==qid]
    #selecting only recent task one
    value_counts = subset['result'].value_counts(dropna=False).reindex(all_categories, fill_value=0)
    tot_num = value_counts.sum()
    abs_num_REF = value_counts.get('REFUTES')
    abs_num_NEI = value_counts.get('NOT ENOUGH INFO')
    ranks.append({'Predicate': qid, 'abs_REFUTES': abs_num_REF, 'abs_num_NEI': abs_num_NEI,'ratio_REFUTES': abs_num_REF/tot_num, 'ratio_NEI': abs_num_NEI/tot_num})

ranks = pd.DataFrame(ranks)
top_n = ranks.sort_values(by=['abs_REFUTES'], ascending=False).iloc[:30]

#2.1. Property ranking based on bad references (refutes)
#Based on the number of bad references

#Based on the ratio of bad references (N(bad refs)/N(all refs))