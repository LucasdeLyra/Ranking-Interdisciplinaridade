import pandas as pd

STAGING_DIR = './data/data/staging'
REFINED_DIR = './data/data/refined'
FILENAMES = ['article_subject_areas', 'article', 'author', 'articles_author', 'authors_institution']
PRIMARY_KEYS = {
   'article_subject_areas': ['eid', 'subject area code'],
   'article': ['eid'],
   'author': ['auid'],
   'articles_author': ['eid', 'auid'],
   'authors_institution': ['affiliation id', 'auid', 'dptid']
}

files = []
for filename in FILENAMES:
    for year in range(2015,2025):
        with open(f'{STAGING_DIR}/{year}/{filename}.csv', 'r', encoding="utf-8") as input_file:
            files.append(pd.read_csv(input_file, dtype=object))
    
    combined = pd.concat(files)
    combined.drop_duplicates(inplace=True, subset=[*PRIMARY_KEYS[filename]])
    
    if filename == 'authors_institution':
        departments = combined[['dptid', 'organization']]
        departments.drop_duplicates(inplace=True, subset=['dptid'])
        departments.dropna(subset=['dptid'], inplace=True)
        combined.drop(columns=['organization'], inplace=True)
        departments.to_csv(f'{REFINED_DIR}/departments.csv', index=False, encoding="utf-8")  

    combined.to_csv(f'{REFINED_DIR}/{filename}.csv', index=False, encoding="utf-8")  
    files = []