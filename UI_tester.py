import gradio as gr
import Wikidata_Text_Parser as wtr
import sqlite3

def process_input(qid):
    progress = gr.Progress(0)
    
    wtr.claimParser(qid)
    
    progress(0.20, desc="Filtering properties...")
    filtered_df = wtr.propertyFiltering(qid)
    
    progress(0.40, desc="Parsing URLs...")
    url_set = wtr.urlParser()
    
    progress(0.60, desc="Parsing HTML...")
    html_set = wtr.htmlParser(url_set)
    
    progress(0.80, desc="Generating claim text...")
    claim_text = wtr.claim2text(html_set) #Claims generation
    
    progress(1, desc="Generating claim text...")
    html_text = wtr.html2text(html_set)

    conn = sqlite3.connect('wikidata_claims_refs_parsed.db')
    claim_text = claim_text.astype(str)
    html_text = html_text.astype(str)
    claim_text.to_sql('claim_text', conn, if_exists='replace', index=False)
    html_text.to_sql('html_text', conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    return f"{html_text.shape[0]} HTMl documents collection via references of {qid}"

with gr.Blocks() as demo:
    gr.Markdown(
        """
        # Reference Quality Verification Tool
        This is a tool for verifying the reference quality of Wikidata claims related to the target entity item.

        Parsing could take 3~5 mins depending on the number of references.
        """
    )
    
    inp = gr.Textbox(label="Input QID", placeholder="Input QID (i.e. Q42)")
    out = gr.Textbox(label="Parsing result")
    run_button = gr.Button("Start parsing")
    run_button.click(process_input, inp, out)



if __name__ == "__main__":
    demo.launch()