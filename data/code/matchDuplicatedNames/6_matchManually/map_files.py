import pandas as pd

EMEC = pd.read_csv(f'./data/data/raw/EMEC/EMEC_institutions.csv', encoding='utf-8', dtype=object)

# Load and prepare data
SCOPUS = pd.read_csv('./data/code/matchDuplicatedNames/3_matchScopusToEMECBySigla/counting.csv', encoding='utf-8')
SCOPUS = SCOPUS[SCOPUS['match_count'] == 0].copy()