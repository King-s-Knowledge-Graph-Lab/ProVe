import pandas as pd
import html2text

SS_df = pd.read_csv('SS_df.csv')
h = html2text.HTML2Text()
h.ignore_links = True
SS_df['html2text2'] = SS_df['html'].apply(lambda x: h.handle(x))
