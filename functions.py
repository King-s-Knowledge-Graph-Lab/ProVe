import sqlite3

def get_filtered_data(db_path, table_name, column_name, filter_value):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = f"SELECT * FROM {table_name} WHERE {column_name} = ?"
    cursor.execute(query, (filter_value,))
    results = cursor.fetchall()
    conn.close()
    return results


db_path = 'reference_checked.db'
qid = 'Q2539'
table_name = 'status'
column_name = 'qid'


##Funtions Examples
#1. checking the queue
data_df = get_filtered_data(db_path, 'status', 'status', 'in queue')

#2. get a list of completed
data_df = get_filtered_data(db_path, 'status', 'status', 'in queue')
