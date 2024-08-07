from functions import generation_worklists
import pandas as pd
import json
import sqlite3
from tqdm import tqdm
worklist = json.loads(generation_worklists())
q_list = pd.DataFrame(worklist['TOP_Cited_Items'])

db_path = 'wikidata_claims_refs_parsed.db'
conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
cursor = conn.cursor()


item_label = []
url_html = []
for i in tqdm(q_list['qid']):
    query = '''
    SELECT entity_id, property_id, entity_label, entity_desc, property_label, property_alias, reference_id
    FROM claim_text
    WHERE entity_id = ?
    '''
    cursor.execute(query, (i,))
    results = pd.DataFrame(cursor.fetchall(), columns=['entity_id', 'property_id', 'entity_label', 'entity_desc', 'property_label', 'property_alias', 'reference_id'])

    query = '''
    SELECT entity_id, reference_id, url, html
    FROM html_text
    WHERE entity_id = ?
    '''
    cursor.execute(query, (i,))
    results_2 = pd.DataFrame(cursor.fetchall(), columns=['entity_id', 'reference_id', 'url', 'html'])
    item_label.append(results)
    url_html.append(results_2)


final_item_label = pd.concat(item_label, ignore_index=True)
final_url_html = pd.concat(url_html, ignore_index=True)

q_list.to_csv('CodeArchive/top_bad_items.csv', index=False, encoding='utf-8-sig')
final_item_label.to_csv('CodeArchive/item_label.csv', index=False, encoding='utf-8-sig')
final_url_html.to_csv('CodeArchive/url_html.csv', index=False, encoding='utf-8-sig')

merged_results = pd.merge(final_item_label, final_url_html[['reference_id', 'url', 'html']], 
                        on='reference_id', 
                        how='left')
final_results = pd.concat(merged_results, ignore_index=True)
#final_results.to_csv('CodeArchive/top_bad_items_urls.csv', index=False, encoding='utf-8-sig')