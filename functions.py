import sqlite3

#Params.
db_path = 'reference_checked.db'

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
def GetItem(target_id):
    check_item = get_filtered_data(db_path, 'status', 'qid', f'{target_id}')
    if len(check_item) != 0:
        getResult_item = get_filtered_data(db_path, 'aggregated_results', 'qid', f'{target_id}')
        if len(getResult_item) ==0:
            getResult_item = [{'Result':'No available URLs'}]
        else:
            keys_to_remove = ['id', 'Results', 'task_id', 'reference_id']
            for item in getResult_item:
                for key in keys_to_remove:
                    item.pop(key, None)
        return check_item + getResult_item
    else:
        return [{'error': 'not processed yet'}]

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
