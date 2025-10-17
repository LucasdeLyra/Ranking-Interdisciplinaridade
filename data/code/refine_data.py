import pandas as pd

STAGING_DIR = './data/data/staging'
REFINED_DIR = './data/data/refined'
FILENAMES = ['article_subject_areas', 'article', 'author', 'authors_institution']
PRIMARY_KEYS = {
    'article_subject_areas': ['eid', 'subject area code'],
    'article': ['eid'],
    'author': ['auid'],
    'authors_institution': [],
    'articles_institution': ['eid','afid'],
    'articles_author':['eid','auid'],
    'departments': ['dptid']
}


for filename in FILENAMES:
    files = []
    for year in range(2015,2025):
        with open(f'{STAGING_DIR}/{year}/{filename}.csv', 'r', encoding="utf-8") as input_file:
            files.append(pd.read_csv(input_file, dtype=object))
    
    combined = pd.concat(files)
    
    if filename != 'authors_institution':
        combined.drop_duplicates(inplace=True, subset=[*PRIMARY_KEYS[filename]])
        combined.to_csv(f'{REFINED_DIR}/{filename}.csv', index=False, encoding="utf-8")  


    else:
        articles_author = combined[['eid', 'auid', 'creator']]
        articles_institution = combined[['eid', 'afid', 'creator', 'country']]
        departments = combined[['afid', 'dptid', 'organization', 'country', 'city']]
        
        articles_institution.drop_duplicates(inplace=True, subset=[*PRIMARY_KEYS['articles_institution']])
        articles_institution.dropna(subset=[*PRIMARY_KEYS['articles_institution']], inplace=True)
        articles_institution.to_csv(f'{STAGING_DIR}/articles_institution.csv', index=False, encoding="utf-8")

        articles_author.drop_duplicates(inplace=True, subset=[*PRIMARY_KEYS['articles_author']])
        articles_author.dropna(subset=[*PRIMARY_KEYS['articles_author']], inplace=True)
        articles_author.to_csv(f'{REFINED_DIR}/articles_author.csv', index=False, encoding="utf-8")
        
        departments.drop_duplicates(inplace=True, subset=[*PRIMARY_KEYS['departments']])
        departments.dropna(subset=[*PRIMARY_KEYS['departments']], inplace=True)
        departments.to_csv(f'{REFINED_DIR}/departments.csv', index=False, encoding="utf-8")