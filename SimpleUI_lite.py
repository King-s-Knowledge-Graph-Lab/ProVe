import gradio as gr
import Wikidata_Text_Parser as wtr
import sqlite3
import Prove_lite as prv
import pandas as pd
import numpy as np

def wtr_process(qid):
    try:
        conn = sqlite3.connect('wikidata_claims_refs_parsed.db')
        target_QID = qid

        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='claims'")
        table_exists = cursor.fetchone()


        cursor.execute("SELECT entity_id FROM claims WHERE entity_id=?", (target_QID,))
        result = cursor.fetchone()
        
        if result is not None and result[0] == target_QID:
            print(result)
            print(f"{target_QID} already exists in the 'claims' table. Skipping execution.")
        else:
            progress = gr.Progress(0)
            progress(0.00, desc="Wikidata claims parsing...")
            wtr.claimParser(target_QID) #save results in .db
            filtered_df = wtr.propertyFiltering(target_QID) #update db and return dataframe after filtering
            progress(0.25, desc="URL and HTML parsing...")
            url_set = wtr.urlParser() #from ref table in .db
            html_set = wtr.htmlParser(url_set, qid) #Original html docs collection
            progress(0.50, desc="claim2Text...")
            claim_text = wtr.claim2text(html_set) #Claims generation
            progress(0.74, desc="html2Text...")
            html_text = wtr.html2text(html_set)
            claim_text = claim_text.astype(str)
            html_text = html_text.astype(str)
            claim_text.to_sql('claim_text', conn, if_exists='replace', index=False)
            html_text.to_sql('html_text', conn, if_exists='replace', index=False)
            progress(1, desc="completed...")

        query = f"""
            SELECT
                claim_text.entity_label,
                claim_text.property_label,
                claim_text.object_label,
                html_text.url
            FROM claim_text
            INNER JOIN html_text ON claim_text.reference_id = html_text.reference_id
            WHERE claim_text.entity_id = '{target_QID}'
        """

        result_df = pd.read_sql_query(query, conn)

        conn.commit()
        conn.close()

        return result_df
    
    except Exception as e:
            error_df = pd.DataFrame({'Error': [str(e)]})
            return error_df


def prv_process(qid):
    target_QID = qid
    conn = sqlite3.connect('wikidata_claims_refs_parsed.db')
    query = f"SELECT * FROM claim_text WHERE entity_id = '{target_QID}'"
    claim_df = pd.read_sql_query(query, conn)
    
    query = f"SELECT * FROM html_text Where  entity_id = '{target_QID}'"
    reference_text_df = pd.read_sql_query(query, conn)
    
    verbalised_claims_df_final = prv.verbalisation(claim_df)

    progress = gr.Progress(len(verbalised_claims_df_final))  # Create progress bar for Gradio
    def update_progress(curr_step, total_steps):
        progress((curr_step + 1) / total_steps)

    splited_sentences_from_html = prv.setencesSpliter(verbalised_claims_df_final, reference_text_df, update_progress)

    BATCH_SIZE = 512
    N_TOP_SENTENCES = 5
    SCORE_THRESHOLD = 0
    evidence_df = prv.evidenceSelection(splited_sentences_from_html, BATCH_SIZE, N_TOP_SENTENCES)
    result = prv.textEntailment(evidence_df, SCORE_THRESHOLD)
    display_df = prv.TableMaking(verbalised_claims_df_final, result)
    conn.commit()
    conn.close()
    return display_df



with gr.Blocks() as demo:
    print("gradio started!")
    gr.Markdown(
        """
        # Prove
        This is a tool for verifying the reference quality of Wikidata claims related to the target entity item.
        """
    )
    inp = gr.Textbox(label="Input QID", placeholder="Input QID (i.e. Q42)")
    out = gr.Dataframe(label="Parsing result (not presenting parsed HTMLs)",  headers=["entity_label", "property_label", "object_label", "url"])
    run_button_1 = gr.Button("Start parsing")
    run_button_1.click(wtr_process, inp, out)


    gr.Markdown(
        """
        Pre-trained language models-based text entailment. 
        """
    )
    out_2 = gr.Dataframe(label="Results")
    run_button_2 = gr.Button("Start processing")
    run_button_2.click(prv_process, inp, out_2)

    
if __name__ == "__main__":
    demo.launch(share=True)