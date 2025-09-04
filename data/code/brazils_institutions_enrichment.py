from pybliometrics.scopus import AffiliationRetrieval
from pybliometrics.scopus import init as ScopusInit
import pandas as pd

ScopusInit()

df = pd.read_csv(f'./data/data/raw/institution.csv')
df['affiliation id'] = df['affiliation id'].astype(int)

for row in df.itertuples(index=False):
    if row.country == 'Brazil':
        print(AffiliationRetrieval(row[0]).name_variants)