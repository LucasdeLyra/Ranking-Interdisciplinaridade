import pandas as pd
import os
files = []
for file in os.listdir('./data/data/raw/EMEC'):
    if file.endswith('.xls'):
        files.append(pd.read_html(f'./data/data/raw/EMEC/{file}')[0])
combined = pd.concat(files)
combined.to_csv(f'./data/data/raw/EMEC/EMEC_institutions.csv', index=False, encoding='utf-8')