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
import requests
from ProVe_main_service import MongoDBHandler

mongo_handler = MongoDBHandler()

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
    try:
        # Check status in MongoDB
        mongo_status = mongo_handler.status_collection.find_one(
            {'qid': target_id},
            sort=[('requested_timestamp', -1)]
        )
        
        if mongo_status:
            task_id = mongo_status['task_id']
            
            # 1. Get initial data structure from html_content collection
            html_contents = list(mongo_handler.html_collection.find(
                {'task_id': task_id},
                {
                    'object_id': 1, 'property_id': 1, 'url': 1, 
                    'entity_label': 1, 'property_label': 1, 'object_label': 1,
                    'reference_id': 1, 'lang': 1, 'status': 1, '_id': 0
                }
            ))
            
            # 2. Transform data structure with new keys and create triple
            result_items = []
            for content in html_contents:
                item = {
                    'qid': content['object_id'],
                    'property_id': content['property_id'],
                    'url': content['url'],
                    'triple': f"{content['entity_label']} {content['property_label']} {content['object_label']}"
                }
                
                # Store these temporarily for processing but don't include in final output
                temp_status = content['status']
                temp_lang = content['lang']
                temp_ref_id = content['reference_id']
                
                # 3. Handle non-200 status codes
                if temp_status != 200:
                    item['result'] = 'error'
                    item['result_sentence'] = f"Source language: ({temp_lang}) / Error code: {temp_status}"
                    result_items.append(item)
                    continue
                
                # 4. Query entailment results using temporary variables
                entailment_results = list(mongo_handler.entailment_collection.find({
                    'task_id': task_id,
                    'reference_id': temp_ref_id
                }))
                
                if entailment_results:
                    # Group by result type and get highest score
                    supports = [r for r in entailment_results if r['result'] == 'SUPPORTS']
                    nei = [r for r in entailment_results if r['result'] == 'NOT ENOUGH INFO']
                    refutes = [r for r in entailment_results if r['result'] == 'REFUTES']
                    
                    selected_result = None
                    if supports:
                        selected_result = max(supports, key=lambda x: x['text_entailment_score'])
                    elif nei:
                        selected_result = max(nei, key=lambda x: x['text_entailment_score'])
                    elif refutes:
                        selected_result = max(refutes, key=lambda x: x['text_entailment_score'])
                    
                    if selected_result:
                        item['result'] = selected_result['result']
                        item['result_sentence'] = f"Source language: ({temp_lang}) / {selected_result['result_sentence']}"
                
                result_items.append(item)
            
            # Format status document
            formatted_status = {
                'qid': mongo_status['qid'],
                'task_id': mongo_status['task_id'],
                'status': mongo_status['status'],
                'algo_version': mongo_status['algo_version'],
                'start_time': mongo_status['requested_timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                if isinstance(mongo_status['requested_timestamp'], datetime) 
                else mongo_status['requested_timestamp']
            }
            
            return [formatted_status] + result_items
            
        # If not found in MongoDB, fallback to SQLite
        return get_item_from_sqlite(target_id)
        
    except Exception as e:
        print(f"Error in GetItem: {e}")
        return [{'error': f'Error retrieving data: {str(e)}'}]

def get_item_from_sqlite(target_id):
    """Existing SQLite search logic"""
    check_item = get_filtered_data(db_path, 'status', 'qid', f'{target_id}')
    if len(check_item) != 0:
        check_item = max(check_item, key=lambda x: x['start_time'][:-5])  # Select the most recent one
        getResult_item = get_filtered_data(db_path, 'aggregated_results', 'task_id', check_item['task_id'])

        if len(getResult_item) == 0:
            getResult_item = [{'Result': 'No available URLs'}]
        else:
            for item in getResult_item:
                if 'result_sentence' in item and 'Error:' in item['result_sentence']:
                    item['result'] = 'error'
                    
            keys_to_remove = ['id', 'Results', 'task_id', 'reference_id']
            for item in getResult_item:
                for key in keys_to_remove:
                    item.pop(key, None)
        return [check_item] + getResult_item
    else:
        return [{'error': 'Not processed yet'}]

def CheckItemStatus(target_id):
    try:
        # Check MongoDB status collection first
        mongo_statuses = list(mongo_handler.status_collection.find({'qid': target_id}))
        
        if mongo_statuses:
            # Get the latest timestamp for each status, handling None values
            def get_latest_timestamp(status_doc):
                timestamps = [
                    status_doc.get('requested_timestamp'),
                    status_doc.get('processing_start_timestamp'),
                    status_doc.get('completed_timestamp')
                ]
                # Convert strings to datetime if necessary
                valid_timestamps = []
                for ts in timestamps:
                    if isinstance(ts, str):
                        try:
                            ts = datetime.fromisoformat(ts)  # 문자열을 datetime으로 변환
                        except ValueError:
                            continue  # 변환할 수 없는 경우 무시
                    if ts is not None:
                        valid_timestamps.append(ts)
                return max(valid_timestamps) if valid_timestamps else datetime.min
            
            latest_status = max(mongo_statuses, key=get_latest_timestamp)
            
            return {
                'qid': latest_status['qid'],
                'status': latest_status['status'],
                'task_id': latest_status.get('task_id'),
                'algo_version': latest_status.get('algo_version')
            }
        
        # If not found in MongoDB, check SQLite
        check_item = get_filtered_data(db_path, 'status', 'qid', f'{target_id}')
        if check_item:
            return check_item[-1]
        
        return {'qid': target_id, 'status': 'Not processed yet'}
        
    except Exception as e:
        print(f"Error in CheckItemStatus: {e}")
        return {'qid': target_id, 'status': 'Error checking status'}
    
#1.2. calculate the reference score for an item
#Examples = Q5820 : error/ Q5208 : good/ Q42220 : None.
def comprehensive_results(target_id):
    """Get comprehensive results for a target ID including reference score and grouped results"""
    response = GetItem(target_id)
    
    if not isinstance(response, list) or not response:
        return None
        
    first_item = response[0]
    task_id = first_item['task_id']
    qid = first_item['qid']
    
    # Fetch total_claims from parser_stats collection
    parser_stats = mongo_handler.stats_collection.find_one(
        {'task_id': task_id, 'entity_id': qid},
        {'total_claims': 1, '_id': 0}
    )
    
    total_claims = parser_stats['total_claims'] if parser_stats else None
    
    # Initialize result structure
    result = {
        'Reference_score': None,
        'REFUTES': None,
        'NOT ENOUGH INFO': None,
        'SUPPORTS': None,
        'error': None,
        'algo_version': first_item.get('algo_version', 'Not processed yet'),
        'Requested_time': first_item.get('start_time', 'Not processed yet'),
        'total_claims': total_claims  # Add total_claims to the result
    }
    
    # Handle special cases
    if 'error' in first_item or first_item.get('status') == 'error':
        result.update({k: 'processing error' for k in result if k not in ['algo_version', 'Requested_time']})
        return result
        
    if len(response) < 2 or response[1].get('Result') == 'No available URLs':
        result.update({k: 'No external URLs' for k in result if k not in ['algo_version', 'Requested_time']})
        return result
    
    # Process normal results
    details = pd.DataFrame(response[1:])
    
    # Calculate counts for SUPPORTS and REFUTES
    supports_count = details[details['result'] == 'SUPPORTS'].shape[0]
    refutes_count = details[details['result'] == 'REFUTES'].shape[0]
    
    # Calculate reference score
    result['Reference_score'] = (supports_count - refutes_count) / total_claims if total_claims else None
    
    # Group results by type
    for result_type in ['REFUTES', 'NOT ENOUGH INFO', 'SUPPORTS', 'error']:
        result[result_type] = details[details['result'] == result_type].to_dict()
    
    return result


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
    """Request processing for a specific QID"""
    try:
        # Check if item is already in queue
        existing_request = mongo_handler.status_collection.find_one({
            'qid': qid,
            'status': 'in queue'
        })
        
        if existing_request:
            return f"QID {qid} is already in queue. Skipping..."
        
        # Create new status document
        status_dict = {
            'qid': qid,
            'task_id': str(uuid.uuid4()),
            'status': 'in queue',
            'algo_version': algo_version,
            'request_type': 'userRequested',
            'requested_timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
            'processing_start_timestamp': None,
            'completed_timestamp': None
        }
        
        # Save to MongoDB
        result = mongo_handler.save_status(status_dict)
        
        return f"Task {status_dict['task_id']} created for QID {qid}"
        
    except Exception as e:
        return f"An error occurred: {e}"

#5. Generation worklist
def finding_latest_entries(full_df):
    latest_tasks = full_df.groupby('qid').apply(lambda x: x.loc[x.index.max()])
    task_list = latest_tasks['task_id'].tolist()
    latest_entries = full_df[full_df['task_id'].isin(task_list)]
    return latest_entries

def sorting_items_based_on_results(latest_entries_site, result_label, group_by, top_n):
    sub_df = latest_entries_site[latest_entries_site['result'] == result_label]
    url_groups = sub_df.groupby(group_by)['url'].apply(list).reset_index(name='url_list')
    item_count = sub_df.groupby(group_by).size().reset_index(name='count')
    merged_df = pd.merge(item_count, url_groups, on=group_by)
    top_selections = merged_df.sort_values('count', ascending=False).head(top_n)
    return top_selections.drop('url_list', axis=1)

def sorting_items_based_on_site(latest_entries_site):
    result_counts = latest_entries_site.groupby('qid')['result'].value_counts().unstack(fill_value=0)
    result_counts.columns.name = None
    result_counts = result_counts.reset_index()
    url = "https://quarry.wmcloud.org/run/888614/output/0/csv"
    df = pd.read_csv(url)
    df = df.rename(columns={'ips_item_id': 'qid', 'count(i.ips_site_id)': 'N_connected_site'})
    df['qid'] = 'Q' + df['qid'].astype(str)
    merged_df = result_counts.merge(df, on='qid', how='left').sort_values('N_connected_site')
    return merged_df


def extract_domain(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc

def dataframe_to_json(df):
    return json.loads(df.to_json(orient='records'))

def generation_worklists():
    full_df = pd.DataFrame(get_full_data(db_path, 'aggregated_results')).set_index('id')
    latest_entries = finding_latest_entries(full_df)
    latest_entries['url_domain'] = latest_entries['url'].apply(extract_domain)
    url = "https://quarry.wmcloud.org/run/888614/output/0/csv"
    df = pd.read_csv(url)
    df = df.rename(columns={'ips_item_id': 'qid', 'count(i.ips_site_id)': 'N_connected_site'})
    df['qid'] = 'Q' + df['qid'].astype(str)
    merged_df = latest_entries.merge(df, on='qid', how='left')
    latest_entries_site = merged_df.sort_values('N_connected_site')
    grouped_table = sorting_items_based_on_site(latest_entries_site)
    grouped_table = grouped_table.sort_values('REFUTES', ascending=False)
    grouped_table = grouped_table.sort_values('N_connected_site', ascending=False)
    result = {
        'TOP_Cited_Items': dataframe_to_json(grouped_table)
    }
    return json.dumps(result)

def generation_worklist_pagePile():
    # Read data from the Excel file
    file_path = 'CodeArchive/resultPagepile.xlsx'
    df = pd.read_excel(file_path)
    
    # Convert the DataFrame to a dictionary format
    data_dict = df.to_dict(orient='records')
    return json.dumps(data_dict, ensure_ascii=False, indent=4)


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

def get_config_as_json():
    """
    Reads the config.yaml file and returns its contents as JSON.
    
    Returns:
        str: JSON string representation of the config.yaml contents
    """
    config = load_config('config.yaml')
    return json.dumps(config, indent=2)

if __name__ == "__main__":
    #requestItemProcessing('Q44')
    GetItem('Q44')
    pass