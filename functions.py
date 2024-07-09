import wikidata_reader
import html_fetching
import reference_checking
import pandas as pd
import os

def save_to_csv(result_df, csv_path):
    if os.path.isfile(csv_path):
        existing_df = pd.read_csv(csv_path, encoding='utf-8-sig')
        updated_df = pd.concat([existing_df, result_df], ignore_index=True)
        updated_df = updated_df.drop_duplicates()
        updated_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    else:
        result_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

def prove_process(qids):
    csv_path = "APIresult.csv"
    wikidata_reader.main(qids, wikidata_reader.config['parsing']['reset_database'])
    html_fetching.main(qids, html_fetching.config)
    result = reference_checking.main(qids)
    save_to_csv(result, csv_path)
    return result

if __name__ == "__main__":
    qids = ["Q42"]
    result = prove_process(qids)   

