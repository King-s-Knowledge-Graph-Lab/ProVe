from datetime import datetime
from functools import partial
from collections import defaultdict
from copy import deepcopy
import json
import sqlite3
from urllib.parse import urlparse
import uuid
from typing import Dict, Any, List

import pandas as pd
from plotly.subplots import make_subplots
from plotly import graph_objects as go
from plotly import io as pio
from pymongo import collection
import yaml

from utils.logger import logger
from utils.mongo_handler import MongoDBHandler
from utils.mongo_handler import requestItemProcessing as request_processing
from utils.objects import Status, HtmlContent, Entailment

mongo_handler = MongoDBHandler()

# Params
def load_config(config_path: str):
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError as e:
        logger.error("Config file not found: %s", e)
        return None

config = load_config('config.yaml')
logger.info("Config loaded successfully: %s", config)

db_path = config['database']['result_db_for_API']
logger.info("Database path: %s", db_path)

algo_version = config['version']['algo_version']
logger.info("Algorithm version: %s", algo_version)


# Table summary
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
logger.info("Table schemas: %s", table_schemas)


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
                    item['result_sentence'] = f"Source language: ({temp_lang}) / HTTP Error code: {temp_status}"
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
        logger.error("Error in GetItem: %s", e)
        return [{'error': f'Error retrieving data: {str(e)}'}]


def get_item(target_id: str, task_id: str = None, header: bool = True) -> List[Dict[str, Any]]:
    try:
        if task_id is None:
            status = mongo_handler.status_collection.find_one(
                {'qid': target_id},
                sort=[('requested_timestamp', -1)]
            )

            if not status:
                return get_item_from_sqlite(target_id)

            status = Status(**status)
            task_id = status.task_id

        html_contents = mongo_handler.html_collection.find({"task_id": task_id})

        items = []
        if html_contents:
            html_contents = [HtmlContent(**html_content) for html_content in html_contents]
            iterable_items = [
                html_content
                for html_content in html_contents
                if html_content.status == 200
            ]

            entailmments = mongo_handler.entailment_collection.aggregate([
                {"$match": {
                    "task_id": task_id,
                    "reference_id": {"$in": [item.reference_id for item in iterable_items]}
                }},
                {"$sort": {"text_entailment_score": -1}},
                {"$group": {
                    "_id": {
                        "reference_id": "$reference_id",
                        "result": "$result"
                    },
                    "docs": {"$push": "$$ROOT"}
                }}
            ])

            entailmments_by_ref = defaultdict(lambda: defaultdict(list))
            for entailmment in entailmments:
                ref_id = entailmment.get("_id").get("reference_id")
                result = entailmment.get("_id").get("result")
                entailmments_by_ref[ref_id][result] = [
                    Entailment(**docs)
                    for docs in entailmment.get("docs")
                ]

            for html_content in iterable_items:
                item_entailments = entailmments_by_ref.get(html_content.reference_id, {})

                if "SUPPORTS" in item_entailments.keys():
                    html_content.add_info_item(item_entailments["SUPPORTS"][0])
                elif "NOT ENOUGH INFO" in item_entailments.keys():
                    html_content.add_info_item(item_entailments["NOT ENOUGH INFO"][0])
                elif "REFUTES" in item_entailments.keys():
                    html_content.add_info_item(item_entailments["REFUTES"][0])

            items = [html_content.item for html_content in html_contents]

        if header and status:
            header_info = {
                "qid": target_id,
                "task_id": task_id,
                "status": status.status,
                "algo_version": status.algo_version,
                "start_time": status.get_formated_requested_timestamp()
            }
            items = [header_info] + items
        elif header:
            logger.error("GetItem with task_id param and header not supported")
            raise NotImplementedError("GetItem with task_id param and header not supported")

        return items
    except Exception as e:
        logger.error("Error in get_item: %s", e)
        return [{'error': f"Error retrieving data: {str(e)}"}]


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
                            ts = datetime.fromisoformat(ts)  # Convert string to datetime
                        except ValueError as e:
                            logger.error("Error converting timestamp: %s", e)  # Ignore if unable to convert
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
        logger.error("Error in CheckItemStatus: %s", e)
        return {'qid': target_id, 'status': 'Error checking status'}


def get_summary(target_id: str, update: bool = False) -> dict[str, any]:
    result = mongo_handler.summary_collection.find_one({'_id': target_id})
    summary = deepcopy(result)

    if result is None or update:
        information = get_item(target_id)

        if not isinstance(information, list) or len(information) == 0:
            return None

        item = information[0]
        if 'error' in item or item.get('status') == 'error':
            return {'status': item.get('status', 'Not processed yet')}

        task_id = item.get("task_id")
        counter = pd.DataFrame(information[1:])

        total_claims = mongo_handler.stats_collection.find_one(
            {'task_id': task_id, 'entity_id': target_id},
            {'total_claims': 1, '_id': 0}
        ).get('total_claims', None)

        version = item.get('algo_version', 'Not processed yet')
        last_update = item.get('start_time', 'Not processed yet')

        result = {
            'algoVersion': version,
            'lastUpdate': last_update,
            'status': 'processed',
            'totalClaims': total_claims,
        }
        if len(information) < 2 or information[1].get('Result') == 'No available URLs':
            result['status'] = 'No available URLs'
            result['proveScore'] = 1.
            mongo_handler.summary_collection.insert_one({'_id': target_id, **result})
            return result

        refuting_count = counter[counter['result'] == 'REFUTES'].shape[0]
        inconclusive_count = counter[counter['result'] == 'NOT ENOUGH INFO'].shape[0]
        supportive_count = counter[counter['result'] == 'SUPPORTS'].shape[0]
        irretrievable_count = counter[counter['result'] == 'error'].shape[0]
        total_counts = sum([refuting_count, inconclusive_count, supportive_count, irretrievable_count])
        prove_score = (supportive_count - refuting_count) / total_counts if total_counts else None

        result.update({
            'proveScore': prove_score,
            'count': {
                'refuting': refuting_count,
                'inconclusive': inconclusive_count,
                'supportive': supportive_count,
                'irretrievable': irretrievable_count,
            }
        })

        if not update or (update and summary is None):
            mongo_handler.summary_collection.insert_one({'_id': target_id, **result})
        else:
            mongo_handler.summary_collection.update_one({'_id': target_id}, {'$set': result})
    else:
        result.pop('_id', None)

    return result


def get_history(
    target_id: str,
    from_date: datetime,
    to_date: datetime,
    index: int,
) -> Dict[str, Any]:

    def get_summary_from_item(
        job: Status,
        items: List[Dict[str, Any]],
        target_id: str
    ) -> Dict[str, Any]:
        counter = pd.DataFrame(items)
        refuting_count = counter[counter['result'] == 'REFUTES'].shape[0]
        inconclusive_count = counter[counter['result'] == 'NOT ENOUGH INFO'].shape[0]
        supportive_count = counter[counter['result'] == 'SUPPORTS'].shape[0]
        irretrievable_count = counter[counter['result'] == 'error'].shape[0]
        total_counts = sum([refuting_count, inconclusive_count, supportive_count, irretrievable_count])
        prove_score = (supportive_count - refuting_count) / total_counts if total_counts else None

        return {
            "algoVersion": job.algo_version,
            "lastUpdate": job.requested_timestamp.isoformat(),
            "status": job.status,
            "totalClaims": mongo_handler.stats_collection.find_one(
                {'task_id': job.task_id, 'entity_id': target_id},
                {'total_claims': 1, '_id': 0}
            ).get('total_claims', None),
            "proveScore": prove_score,
            "count": {
                "refuting": refuting_count,
                "inconclusive": inconclusive_count,
                "supportive": supportive_count,
                "irretrievable": irretrievable_count,
            }
        }

    jobs = mongo_handler.status_collection.find({'qid': target_id}).sort("completed_timestamp", -1)
    if jobs:
        jobs = [Status(**job) for job in jobs]
        jobs = [job for job in jobs if job.status == 'completed']

        if len(jobs) == 0:
            return {}

        if from_date:
            jobs = [job for job in jobs if from_date <= job <= to_date]

            history = {}
            for job in jobs:
                items = get_item(target_id, job.task_id, header=False)
                items = get_summary_from_item(job, items, target_id)
                history[job.completed_timestamp.isoformat()] = items
            return history

        if index is not None:
            if index > len(jobs):
                index = len(jobs)

            if index == 0:
                job = jobs[0]
                return {
                    job.completed_timestamp.isoformat(): get_summary_from_item(
                        job, get_item(target_id, job.task_id, header=False), target_id
                    )
                }
            else:
                iterable_jobs = jobs[:index + 1]
                history = {}
                for job in iterable_jobs:
                    items = get_item(target_id, job.task_id, header=False)
                    items = get_summary_from_item(job, items, target_id)
                    history[job.completed_timestamp.isoformat()] = items
                return history
    return {}


#1.2. calculate the reference score for an item
#Examples = Q5820 : error/ Q5208 : good/ Q42220 : None.
def comprehensive_results(target_id):
    """Get comprehensive results for a target ID including reference score and grouped results"""
    response = get_item(target_id)

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
    
    # Calculate counts for SUPPORTS, REFUTES, NOT ENOUGH INFO, and ERROR
    supports_count = details[details['result'] == 'SUPPORTS'].shape[0]
    refutes_count = details[details['result'] == 'REFUTES'].shape[0]
    not_enough_info_count = details[details['result'] == 'NOT ENOUGH INFO'].shape[0]
    error_count = details[details['result'] == 'error'].shape[0]

    # Calculate reference score using the sum of all relevant counts
    total_counts = supports_count + refutes_count + not_enough_info_count + error_count

    # Calculate reference score
    result['Reference_score'] = (supports_count - refutes_count) / total_counts if total_counts else None
    
    # Group results by type
    for result_type in ['REFUTES', 'NOT ENOUGH INFO', 'SUPPORTS', 'error']:
        result[result_type] = details[details['result'] == result_type].to_dict()
    
    return result


#2. status
#2.1. checkQueue
def checkQueue():
    in_queue = mongo_handler.user_collection.find(
        {'status': 'in queue'},
        sort=[('requested_timestamp', 1)]
    )

    items = []
    for item in in_queue:
        items.append({
            'item': item['qid'],
            'timestamp': item['requested_timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%f'),
        })
    return items

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


def requestItemProcessing(qid: str):
    """Request processing for a specific QID"""
    save_function = partial(
        mongo_handler.save_status,
        queue=mongo_handler.user_collection,
    )
    return request_processing(
        qid=qid,
        algo_version=algo_version,
        request_type="userRequested",
        queue=mongo_handler.user_collection,
        save_function=save_function
    )


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

def process_reference(url: str, claim: str) -> Dict[str, Any]:
    import nltk
    import requests
    import pandas as pd
    import ProVe_main_process
    from refs_html_collection import HTMLFetcher
    from refs_html_to_evidences import HTMLSentenceProcessor, EvidenceSelector
    from claim_entailment import ClaimEntailmentChecker

    nltk.data.path.append('/home/ubuntu/nltk_data/')

    model = ProVe_main_process.initialize_models()
    text_entailment, sentence_retrieval, verb_module = model

    selector = EvidenceSelector(
        sentence_retrieval=sentence_retrieval,
        verb_module=verb_module,
    )
    checker = ClaimEntailmentChecker(text_entailment=text_entailment)

    fetcher = HTMLFetcher(config_path="/home/ubuntu/RQV/config.yaml")
    html_result = requests.get(url, timeout=fetcher.timeout, headers=fetcher.headers)
    result = {
        "status": [html_result.status_code],
        "html": [html_result.text],
        "url": [url],
        "reference_id": ["Q42"],
    }
    source = {
        "verbalisation_unks_replaced_then_dropped": [claim],
        "claims_refs": [url],
        "reference_id": ["Q42"],
    }
    source_df = pd.DataFrame.from_dict(source)
    html_df = pd.DataFrame.from_dict(result)

    processor = HTMLSentenceProcessor()
    sentence_df = processor.process_html_to_sentences(html_df)

    evidence_df = selector.select_relevant_sentences(source_df, sentence_df)

    df = checker.process_entailment(evidence_df, html_df, "Q42")

    if "SUPPORTS" in df["result"].to_list():
        result = df.loc[
            (df['result'] == 'SUPPORTS') &
            (df['text_entailment_score'] == df.loc[df['result'] == 'SUPPORTS', 'text_entailment_score'].max())
        ]
    elif "NOT ENOUGH INFO" in df["result"].to_list():
        result = df.loc[
            (df['result'] == 'NOT ENOUGH INFO') &
            (df['text_entailment_score'] == df.loc[df['result'] == 'NOT ENOUGH INFO', 'text_entailment_score'].max())
        ]
    else:
        result = df.loc[
            (df['result'] == 'REFUTES') &
            (df['text_entailment_score'] == df.loc[df['result'] == 'REFUTES', 'text_entailment_score'].max())
        ]

    result = result.reset_index(drop=True).to_dict()
    return {
        "score": result["text_entailment_score"][0],
        "sentence": result["result_sentence"][0],
        "similarity": result["similarity_score"][0],
        "result": result["result"][0],
    }

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
    try:
        config = load_config('config.yaml')
        return json.dumps(config, indent=2)
    except FileNotFoundError as e:
        logger.error("Config file not found: %s", e)
        return None


def process_reference(url: str, claim: str):
    import nltk
    import requests
    import pandas as pd
    import ProVe_main_process
    from refs_html_collection import HTMLFetcher
    from refs_html_to_evidences import HTMLSentenceProcessor, EvidenceSelector
    from claim_entailment import ClaimEntailmentChecker

    nltk.data.path.append('/home/ubuntu/nltk_data/')

    model = ProVe_main_process.initialize_models()
    text_entailment, sentence_retrieval, verb_module = model

    selector = EvidenceSelector(
        sentence_retrieval=sentence_retrieval,
        verb_module=verb_module,
    )
    checker = ClaimEntailmentChecker(text_entailment=text_entailment)

    fetcher = HTMLFetcher(config_path="/home/ubuntu/RQV/config.yaml")
    html_result = requests.get(url, timeout=fetcher.timeout, headers=fetcher.headers)
    result = {
        "status": [html_result.status_code],
        "html": [html_result.text],
        "url": [url],
        "reference_id": ["Q42"],
    }
    source = {
        "verbalisation_unks_replaced_then_dropped": [claim],
        "claims_refs": [url],
        "reference_id": ["Q42"],
    }
    source_df = pd.DataFrame.from_dict(source)
    html_df = pd.DataFrame.from_dict(result)

    processor = HTMLSentenceProcessor()
    sentence_df = processor.process_html_to_sentences(html_df)

    evidence_df = selector.select_relevant_sentences(source_df, sentence_df)

    df = checker.process_entailment(evidence_df, html_df, "Q42")

    if "SUPPORTS" in df["result"].to_list():
        result = df.loc[
            (df['result'] == 'SUPPORTS') &
            (df['text_entailment_score'] == df.loc[df['result'] == 'SUPPORTS', 'text_entailment_score'].max())
        ]
    elif "NOT ENOUGH INFO" in df["result"].to_list():
        result = df.loc[
            (df['result'] == 'NOT ENOUGH INFO') &
            (df['text_entailment_score'] == df.loc[df['result'] == 'NOT ENOUGH INFO', 'text_entailment_score'].max())
        ]
    else:
        result = df.loc[
            (df['result'] == 'REFUTES') &
            (df['text_entailment_score'] == df.loc[df['result'] == 'REFUTES', 'text_entailment_score'].max())
        ]

    result = result.reset_index(drop=True).to_dict()
    return {
        "score": result["text_entailment_score"][0],
        "sentence": result["result_sentence"][0],
        "similarity": result["similarity_score"][0],
        "result": result["result"][0],
    }


if __name__ == "__main__":
    #requestItemProcessing('Q44')
    process_reference("https://discovering.beer/what-beer-is-made-from/hops/", "beer no part(s) hops")
