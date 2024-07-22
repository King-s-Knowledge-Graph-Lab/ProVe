import sqlite3
import pandas as pd
from datetime import datetime
import yaml

#Params.
def load_config(config_path: str):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)
config = load_config('config.yaml')
db_path = config['database']['result_db_for_API']

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


##Funtions Examples
#1. items
#1.1. check the aggregated results for an item (only recent one)
def GetItem(target_id):
    check_item = get_filtered_data(db_path, 'status', 'qid', f'{target_id}')
    if len(check_item) != 0:
        check_item = max(check_item, key=lambda x: datetime.fromisoformat(x['start_time'])) #select recent one
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
        return [{'error': 'not processed yet'}]
    
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
