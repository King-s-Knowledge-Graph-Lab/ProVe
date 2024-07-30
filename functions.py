import sqlite3
import pandas as pd
from datetime import datetime
import yaml
import uuid
from urllib.parse import urlparse
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio



#Params.
def load_config(config_path: str):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)
config = load_config('config.yaml')

db_path = config['database']['result_db_for_API']
algo_version = config['version']['algo_version']

#Table summary
def get_all_tables_and_schemas(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    table_schemas = {}
    for (table_name,) in tables:
        cursor.execute(f"PRAGMA table_info({table_name});")
        schema = cursor.fetchall()
        table_schemas[table_name] = schema

    conn.close()
    return table_schemas

def print_schemas(table_schemas):
    for table_name, schema in table_schemas.items():
        print(f"\nTable: {table_name}")
        print("Column Information:")
        for column in schema:
            print(f"  Name: {column[1]}, Type: {column[2]}, NotNull: {column[3]}, DefaultVal: {column[4]}, PK: {column[5]}")

table_schemas = get_all_tables_and_schemas(db_path)


print("\nSchema information for each table:")
print_schemas(table_schemas)

#Utils
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

def get_full_data(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    data = [dict(zip(columns, row)) for row in results]
    return data


##Funtions Examples
#1. items
#1.1. check the aggregated results for an item (only recent one)
def GetItem(target_id):
    check_item = get_filtered_data(db_path, 'status', 'qid', f'{target_id}')
    if len(check_item) != 0:
        check_item = max(check_item, key=lambda x: x['start_time'][:-5])#select recent one
        getResult_item = get_filtered_data(db_path, 'aggregated_results', 'task_id', check_item['task_id'])

        if len(getResult_item) ==0:
            getResult_item = [{'Result':'No available URLs'}]
        else:
            keys_to_remove = ['id', 'Results', 'task_id', 'reference_id']
            for item in getResult_item:
                for key in keys_to_remove:
                    item.pop(key, None)
        return [check_item] + getResult_item
    else:
        return [{'error': 'Not processed yet'}]

def CheckItemStatus(target_id):
    check_item = get_filtered_data(db_path, 'status', 'qid', f'{target_id}')
    if len(check_item) != 0:
        return check_item[-1]
    else:
        return [{'qid': target_id, 'status': 'Not processed yet'}]
    
    
#1.2. calculate the reference healthy value for an item
#Examples = Q5820 : error/ Q5208 : good/ Q42220 : None.
def comprehensive_results(target_id):
    response = GetItem(target_id)
    if isinstance(response, list) and len(response) > 0:
        first_item = response[0]
        if isinstance(first_item, dict):
            if 'error' in first_item:
                return {'health_value': 'Not processed yet', 
                        'NOT ENOUGH INFO': 'Not processed yet',
                        'SUPPORTS': 'Not processed yet',
                        'REFUTES': 'Not processed yet'
                        }
            elif 'status' in first_item and first_item['status'] == 'error':
                return {'health_value': 'processing error', 
                        'NOT ENOUGH INFO': 'processing error',
                        'SUPPORTS': 'processing error',
                        'REFUTES': 'processing error'
                        }
            elif response[1].get('Result') == 'No available URLs':
                return {'health_value': 'No external URLs', 
                        'NOT ENOUGH INFO': 'No external URLs',
                        'SUPPORTS': 'No external URLs',
                        'REFUTES': 'No external URLs'
                        }
            else:
                details =  pd.DataFrame(response[1:])
                chekck_value_counts = details['result'].value_counts() 
                health_value = 1-((chekck_value_counts.get('REFUTES', 0)+ chekck_value_counts.get('NOT ENOUGH INFO', 0)*0.5)/chekck_value_counts.sum())
                return {'health_value': health_value, 
                        'REFUTES': details[details['result']=='REFUTES'].to_dict(), 
                        'NOT ENOUGH INFO': details[details['result']=='NOT ENOUGH INFO'].to_dict(),
                        'SUPPORTS': details[details['result']=='SUPPORTS'].to_dict()
                        }


#2. status
#2.1. checkQueue
def checkQueue():
    data_df = get_filtered_data(db_path, 'status', 'status', 'in queue')
    data_df = [{k: v for k, v in item.items() if k not in 'algo_version'} for item in data_df]
    return data_df
#2.2. checkCompleted
def checkCompleted():
    data_df = get_filtered_data(db_path, 'status', 'status', 'completed')
    data_df = [{k: v for k, v in item.items() if k not in 'algo_version'} for item in data_df]
    return data_df
#2.3. checkErrors
def checkErrors():
    data_df = get_filtered_data(db_path, 'status', 'status', 'error')
    data_df = [{k: v for k, v in item.items() if k not in 'algo_version'} for item in data_df]
    return data_df
#2.4. checkParams

#3. statistics
data_df = get_filtered_data(db_path, 'status', 'status', 'in queue')

#4. requests
def update_status(conn, qid, status, algo_version, request_type):
    cursor = conn.cursor()
    task_id = str(uuid.uuid4())
    start_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    cursor.execute('''
        INSERT INTO status (task_id, qid, status, start_time, algo_version, request_type)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (task_id, qid, status, start_time, algo_version, request_type))
    conn.commit()
    return task_id

def get_queued_qids(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT task_id, qid, start_time FROM status WHERE status = "in queue"')
    return [(row[0], row[1], row[2]) for row in cursor.fetchall()]

def check_queue_status(conn, qid):
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM status WHERE qid = ? AND status = "in queue"', (qid,))
    count = cursor.fetchone()[0]
    return count > 0

def requestItemProcessing(qid):
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=100)
        if check_queue_status(conn, qid):
            return f"QID {qid} is already in queue. Skipping..."
        task_id = update_status(conn, qid, "in queue", algo_version, 'user_request')
        queued_tasks = get_queued_qids(conn)
        conn.commit() 
        return f"Task {task_id} created for QID {qid}"
    except sqlite3.Error as e:
        if conn:
            conn.rollback()  
        return f"An error occurred: {e}"
    finally:
        if conn:
            conn.close()

#5. Generation worklist
def finding_latest_entries(full_df):
    latest_tasks = full_df.groupby('qid').apply(lambda x: x.loc[x.index.max()])
    task_list = latest_tasks['task_id'].tolist()
    latest_entries = full_df[full_df['task_id'].isin(task_list)]
    return latest_entries

def sorting_items_based_on_results(latest_entries, result_label, group_by, top_n):
    sub_df = latest_entries[latest_entries['result'] == result_label]
    url_groups = sub_df.groupby(group_by)['url'].apply(list).reset_index(name='url_list')
    item_count = sub_df.groupby(group_by).size().reset_index(name='count')
    merged_df = pd.merge(item_count, url_groups, on=group_by)
    top_selections = merged_df.sort_values('count', ascending=False).head(top_n)
    return top_selections.drop('url_list', axis=1)

def extract_domain(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc

def dataframe_to_json(df):
    return json.loads(df.to_json(orient='records'))

def generation_worklists():
    full_df = pd.DataFrame(get_full_data(db_path, 'aggregated_results')).set_index('id')
    latest_entries = finding_latest_entries(full_df)
    latest_entries['url_domain'] = latest_entries['url'].apply(extract_domain)
    result = {
        'TOP_NOT_ENOUGH_INFO_ITEMS': dataframe_to_json(sorting_items_based_on_results(latest_entries, 'NOT ENOUGH INFO', 'qid', 100)),
        'TOP_REFUTES_ITEMS': dataframe_to_json(sorting_items_based_on_results(latest_entries, 'REFUTES', 'qid', 100)),
        'TOP_NOT_ENOUGH_INFO_PROP': dataframe_to_json(sorting_items_based_on_results(latest_entries, 'NOT ENOUGH INFO', 'qid', 100)),
        'TOP_REFUTES_ITEMS_PROP': dataframe_to_json(sorting_items_based_on_results(latest_entries, 'REFUTES', 'qid', 100)),
    }
    return json.dumps(result)

def plot_status():
    def extract_hour(x):
        return x[11:13]
    status_df = pd.DataFrame(get_full_data(db_path, 'status'))
    result_df = pd.DataFrame(get_full_data(db_path, 'aggregated_results')).set_index('id')
    status_subset = status_df[['task_id', 'start_time']].set_index('task_id')
    result_df = result_df.join(status_subset, on='task_id')

    #Fisrt plot
    status_df['hour'] = status_df['start_time'].apply(extract_hour)
    hourly_status = status_df.groupby('hour')['status'].value_counts().unstack(fill_value=0)

    #Second plot
    result_df['hour'] = result_df['start_time'].apply(extract_hour)
    hourly_result_df= result_df.groupby('hour')['result'].value_counts().unstack(fill_value=0)

    #Trhid plot
    hourly_status_request = status_df.groupby('hour')['request_type'].value_counts().unstack(fill_value=0)


    # Create subplots
    fig = make_subplots(rows=3, cols=1, subplot_titles=("Hourly Status Count", "Hourly Result Count", "Hourly Request Type Count"))

    # First plot: Status
    fig.add_trace(
        go.Scatter(x=hourly_status.index, y=hourly_status['completed'], name="Completed"),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=hourly_status.index, y=hourly_status['error'], name="Error"),
        row=1, col=1
    )

    # Second plot: Results
    for result_type in hourly_result_df.columns:
        fig.add_trace(
            go.Scatter(x=hourly_result_df.index, y=hourly_result_df[result_type], name=result_type),
            row=2, col=1
        )

    # Third plot: Request Types
    for request_type in hourly_status_request.columns:
        fig.add_trace(
            go.Scatter(x=hourly_status_request.index, y=hourly_status_request[request_type], name=f"Request: {request_type}"),
            row=3, col=1
        )

    # Update layout
    fig.update_layout(
        title_text="Hourly Status, Result, and Request Type Counts",
        height=1200,  # Increase height to accommodate three subplots
        hovermode="x unified"
    )

    # Update axes
    for i in range(1, 4):
        fig.update_xaxes(title_text="Hour", row=i, col=1)
        fig.update_yaxes(title_text="Count", row=i, col=1)

    # Adjust legend
    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="bottom",
        y=-0.15,
        xanchor="center",
        x=0.5
    ))

    plot_html = pio.to_html(fig, full_html=True, include_plotlyjs=True)
    return plot_html