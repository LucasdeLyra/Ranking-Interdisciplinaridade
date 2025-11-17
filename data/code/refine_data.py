"""
Data Refinement and Consolidation Module
========================================

Purpose:
    Consolidates multi-year SCOPUS article data from staging directory into
    refined CSV files with deduplication and normalization.

Description:
    This module reads raw SCOPUS data from individual year directories,
    concatenates records across years (2015-2024), removes duplicates based
    on primary keys, and exports consolidated datasets to the refined directory.
    
    For authors_institution data, also extracts and normalizes three separate
    dimensional tables: articles_author relationships, articles_institution 
    relationships, and departments.

Data Flow:
    1. Read year-specific CSV files from staging directory (2015-2024)
    2. Concatenate all years into single DataFrame
    3. Deduplicate based on defined primary keys
    4. For authors_institution: Extract and normalize three separate tables
    5. Export to refined directory with UTF-8 encoding

Output Files:
    Standard consolidation:
        - article_subject_areas.csv
        - article.csv
        - author.csv
    
    Derived from authors_institution:
        - articles_author.csv (article-author relationships)
        - articles_institution.csv (article-institution relationships)
        - departments.csv (department information)

Author: Lucas de Lyra
Project: Ranking-Interdisciplinaridade (USP RP2)
"""

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


"""
Main processing loop: Consolidate data for each filename across all years.

For each dataset:
1. Load all year-specific CSV files into DataFrames
2. Concatenate into single multi-year DataFrame
3. Handle two distinct processing paths:
   - Standard datasets: Simple deduplication and export
   - authors_institution: Extract three normalized relational tables
"""
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