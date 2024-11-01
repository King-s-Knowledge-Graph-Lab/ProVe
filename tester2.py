import pandas as pd
from LLM_translation import translate_text  
import pdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True  
)
logger = logging.getLogger(__name__)

SS_df = pd.read_csv('SS_df.csv')

urls_to_translate = SS_df[~SS_df['language'].str.contains('en|unknown', case=False, na=False)]['url'].unique()

total_urls = len(urls_to_translate)
for idx, url in enumerate(urls_to_translate, 1):
    try:
        text_to_translate = SS_df[SS_df['url'] == url]['html2text'].iloc[0]
        translated_text = translate_text(text_to_translate)
        SS_df.loc[SS_df['url'] == url, 'html2text'] = translated_text
        logger.info(f"Successfully translated URL {idx}/{total_urls}")
    except Exception as e:
        logger.error(f"Error translating URL {idx}/{total_urls} ({url}): {str(e)}")
pdb.set_trace()