import wikidata_reader
import html_fetching
import reference_checking
import pandas as pd
import sqlite3
import os
from SPARQLWrapper import SPARQLWrapper, JSON
import random
import datetime
import time
import uuid
import yaml
import schedule

def save_to_sqlite(result_df, db_path, table_name):
    result_df = result_df.astype(str)
    conn = sqlite3.connect(db_path)
    try:
        # Insert new data
        result_df.to_sql(table_name, conn, if_exists='append', index=False)

        # Remove duplicates, considering both task_id and id
        conn.execute(f"""
        DELETE FROM {table_name}
        WHERE (task_id, id) NOT IN (
            SELECT task_id, MAX(id)
            FROM {table_name}
            GROUP BY task_id, id
        )
        """)

        conn.commit()
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()

def initialize_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS original_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        final_verbalisation TEXT,
        url TEXT,
        nlp_sentences TEXT,
        nlp_sentences_slide_2 TEXT,
        nlp_sentences_scores TEXT,
        nlp_sentences_slide_2_scores TEXT,
        nlp_sentences_TOP_N TEXT,
        nlp_sentences_slide_2_TOP_N TEXT,
        nlp_sentences_all_TOP_N TEXT,
        evidence_TE_prob_TOP_N TEXT,
        evidence_TE_prob_weighted_TOP_N TEXT,
        evidence_TE_labels_TOP_N TEXT,
        claim_TE_prob_weighted_sum_TOP_N TEXT,
        claim_TE_label_weighted_sum_TOP_N TEXT,
        claim_TE_label_malon_TOP_N TEXT,
        evidence_TE_prob_slide_2_TOP_N TEXT,
        evidence_TE_prob_weighted_slide_2_TOP_N TEXT,
        evidence_TE_labels_slide_2_TOP_N TEXT,
        claim_TE_prob_weighted_sum_slide_2_TOP_N TEXT,
        claim_TE_label_weighted_sum_slide_2_TOP_N TEXT,
        claim_TE_label_malon_slide_2_TOP_N TEXT,
        evidence_TE_prob_all_TOP_N TEXT,
        evidence_TE_prob_weighted_all_TOP_N TEXT,
        evidence_TE_labels_all_TOP_N TEXT,
        claim_TE_prob_weighted_sum_all_TOP_N TEXT,
        claim_TE_label_weighted_sum_all_TOP_N TEXT,
        claim_TE_label_malon_all_TOP_N TEXT,
        qid TEXT,
        processed_timestamp TEXT,
        task_id TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS aggregated_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        triple TEXT,
        property_id TEXT,
        url TEXT,
        Results TEXT,
        qid TEXT,
        reference_id TEXT,
        task_id TEXT,
        result TEXT,
        result_sentence TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reformedHTML_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        qid TEXT,
        HTML TEXT,
        task_id TEXT
    )
    """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS status (
        task_id TEXT PRIMARY KEY,
        qid TEXT,
        status TEXT,
        start_time TEXT,
        algo_version TEXT,
        request_type TEXT
    )
    """)


    conn.commit()
    conn.close()

def get_random_qids(num_qids, max_retries, delay):
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setQuery("""
        SELECT ?item {
        SERVICE bd:sample {
            ?item wikibase:sitelinks [].
            bd:serviceParam bd:sample.limit "100".
        }
            MINUS {?item wdt:P31/wdt:P279* wd:Q4167836.}
            MINUS {?item wdt:P31/wdt:P279* wd:Q4167410.}
            MINUS {?item wdt:P31 wd:Q13406463.}
            MINUS {?item wdt:P31/wdt:P279* wd:Q11266439.}
            MINUS {?item wdt:P31 wd:Q17633526.}
            MINUS {?item wdt:P31 wd:Q13442814.}
            MINUS {?item wdt:P3083 [].}
            MINUS {?item wdt:P1566 [].}
            MINUS {?item wdt:P442 [].}
        }
    """)
    sparql.setReturnFormat(JSON)

    for attempt in range(max_retries):
        try:
            results = sparql.query().convert()
            all_qids = [result["item"]["value"].split("/")[-1] for result in results["results"]["bindings"]]
            
            # Randomly select the required number of QIDs
            if len(all_qids) >= num_qids:
                return random.sample(all_qids, num_qids)
            else:
                return all_qids  # Return all if we have fewer than requested
        
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Error occurred: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"Failed to fetch QIDs after {max_retries} attempts.")
                return []  # Return an empty list if all attempts fail

    return []  # This line should never be reached, but it's here for completeness

def get_popular_connected_qids(num_qids):
    file_path = 'CodeArchive/prior_item_list.csv'  # Add .csv extension
    url = "https://quarry.wmcloud.org/run/888614/output/0/csv"
    def format_qid(qid):
        return 'Q' + str(qid).lstrip('Q')
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        df['qid'] = df['qid'].apply(format_qid)  # Ensure correct QID format when reading
    else:
        df = pd.read_csv(url)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df = df.rename(columns={'ips_item_id': 'qid', 'count(i.ips_site_id)': 'N_connected_site'})
        df['qid'] = df['qid'].apply(format_qid)  # Format QID correctly

    df_sorted = df.sort_values('N_connected_site', ascending=False).reset_index(drop=True)
    top_n = df_sorted.head(num_qids)
    df_remaining = df_sorted[num_qids:]
    top_n_qids = top_n['qid'].tolist()
    df_remaining.to_csv(file_path, index=False)
    return top_n_qids
    
    

def update_status(conn, qid, status, algo_version, request_type):
    cursor = conn.cursor()
    task_id = str(uuid.uuid4())  # Generate a random UUID
    start_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
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

def prove_process(db_path, batch_qids, algo_version):
    original_results, aggregated_results, reformedHTML_results = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    try:
        conn = sqlite3.connect(db_path)
        if len(get_queued_qids(conn)) < batch_qids:
            """
            #qids = get_random_qids(batch_qids, 10, 1)
            #qids = get_popular_connected_qids(batch_qids)  
            for qid in qids:
                task_id = update_status(conn, qid, "in queue", algo_version, 'random_running')
            """
        qids = get_popular_connected_qids(batch_qids)  
        for qid in qids:
            task_id = update_status(conn, qid, "in queue", algo_version, 'from_pagepile')
        queued_tasks = get_queued_qids(conn)[:batch_qids] 
        queued_qids = [qid for _, qid, _ in queued_tasks]
        task_ids = [task_id for task_id, _, _ in queued_tasks]
        print(f"Tasks in queue: {queued_tasks}")

        if queued_qids:
            # Process all queued QIDs in batch
            print(f"Processing QIDs: {queued_qids}")
            wikidata_reader.main(queued_qids)
            html_fetching.main(queued_qids)
            batch_original, batch_aggregated, batch_reformedHTML = reference_checking.main(queued_qids)

            # Process results for each QID
            for qid, task_id in zip(queued_qids, task_ids):
                print(f"Saving results for QID {qid} with Task ID {task_id}")
                
                # Filter results for the current QID
                task_original = batch_original[batch_original['qid'] == qid] if not batch_original.empty else pd.DataFrame()
                task_aggregated = batch_aggregated[batch_aggregated['qid'] == qid] if not batch_aggregated.empty else pd.DataFrame()
                task_reformedHTML = batch_reformedHTML[batch_reformedHTML['qid'] == qid] if not batch_reformedHTML.empty else pd.DataFrame()

                with sqlite3.connect(db_path) as task_conn:
                    if not task_original.empty:
                        task_original['task_id'] = task_id
                        save_to_sqlite(task_original, db_path, 'original_results')
                    if not task_aggregated.empty:
                        task_aggregated['task_id'] = task_id
                        save_to_sqlite(task_aggregated, db_path, 'aggregated_results')
                    if not task_reformedHTML.empty:
                        task_reformedHTML['task_id'] = task_id
                        #save_to_sqlite(task_reformedHTML, db_path, 'reformedHTML_results')

                    cursor = task_conn.cursor()
                    cursor.execute('''
                    UPDATE status
                    SET status = ?, start_time = ?
                    WHERE task_id = ?
                    ''', ("completed", datetime.datetime.now().isoformat(), task_id))
                
                # Concatenate results
                original_results = pd.concat([original_results, task_original])
                aggregated_results = pd.concat([aggregated_results, task_aggregated])
                reformedHTML_results = pd.concat([reformedHTML_results, task_reformedHTML])
                
                print(f"Completed processing QID {qid} with Task ID {task_id}")

        else:
            print("No QIDs in queue to process.")
            time.sleep(4)


    except KeyboardInterrupt:
        print("Process interrupted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
        for task_id, qid in zip(task_ids, queued_qids):
            with sqlite3.connect(db_path) as error_conn:
                cursor = error_conn.cursor()
                cursor.execute('''
                UPDATE status
                SET status = ?, start_time = ?
                WHERE task_id = ?
                ''', ("error", datetime.datetime.now().isoformat(), task_id))
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def load_config(config_path: str):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def update_prior_item_list():
    pagepile_df = pd.read_csv('CodeArchive/pagepile.csv', header=None)
    prior_item_list_df = pd.read_csv('CodeArchive/prior_item_list.csv')
    prior_item_list_df['qid'] = pagepile_df[0]

    prior_item_list_df.to_csv('CodeArchive/prior_item_list.csv', index=False)

    print("prior_item_list.csv has been successfully updated.")

def backup_database():
    """
    Backup the reference_checked.db file to the specified HPC directory
    with date prefix in format YYYYMMDD_reference_checked.db
    """
    try:
        # Source database path
        source_db = 'reference_checked.db'
        
        # Create backup directory if it doesn't exist
        backup_dir = '/hpc/scratch/prj/inf_wqp/prove_backup'
        os.makedirs(backup_dir, exist_ok=True)
        
        # Generate backup filename with date prefix
        date_prefix = datetime.datetime.now().strftime('%Y%m%d')
        backup_filename = f"{date_prefix}_reference_checked.db"
        backup_path = os.path.join(backup_dir, backup_filename)
        
        # Copy the database file
        import shutil
        shutil.copy2(source_db, backup_path)
        print(f"Database backup created successfully at {backup_path}")
        
    except Exception as e:
        print(f"Error during database backup: {e}")

def main(batch_qids):
    reset_database = False  # Developer mode to test, it initialize db for getting clean db
    config = load_config('config.yaml')
    db_path = config['database']['result_db_for_API']
    algo_version = config['version']['algo_version']
    if reset_database and os.path.exists(db_path):
        os.remove(db_path)
        print(f"Database file {db_path} has been deleted.")
    
    initialize_database(db_path)
    
    # Schedule both tasks for Monday
    schedule.every().monday.do(update_prior_item_list)
    schedule.every().monday.do(backup_database)
    
    while True:
        try:
            prove_process(db_path, batch_qids, algo_version)
            schedule.run_pending()
        except Exception as e:
            print(f"An error occurred in the main loop: {e}")
            time.sleep(30)
    

if __name__ == "__main__":
    batch_qids = 2
    main(batch_qids)
    
    # nohup python3 eventHandler.py > output.log 2>&1 &
    # nohup python3 -u eventHandler.py > output.log 2>&1 &