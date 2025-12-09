from collections import defaultdict
import pandas as pd
import ast
import re


EMEC = pd.read_csv(f'./data/data/raw/EMEC/EMEC_institutions.csv', encoding='utf-8', dtype=object)
SCOPUS = pd.read_csv(f'./data/data/staging/aux_institutions.csv', encoding='utf-8', dtype=object)

def normalize_site(url):
    if pd.isna(url):
        return url
    url = str(url).lower().strip()
    url =  url.replace('http://', '')
    url =  url.replace('https://', '') 
    url =  url.replace('www.', '')
    url =  url.replace('www2.', '')
    url =  url.replace('www3.', '')
    url =  url.replace('www4.', '')
    url =  url.replace('www5.', '')
    if url.endswith('/'):
        url = url.rstrip('/')
    return url

EMEC['normalized_Sitio'] = EMEC['Sitio'].apply(normalize_site)
SCOPUS['normalized_domain'] = SCOPUS['domain'].apply(normalize_site)
SCOPUS['normalized_url'] = SCOPUS['url'].apply(normalize_site)

EMEC['normalized_Instituição(IES)'] = EMEC['Instituição(IES)'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower().apply(lambda x: re.sub(r'\s*\([^)]*\)', '', x))
SCOPUS['normalized_variants'] = SCOPUS['variants'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower().apply(lambda x: re.sub(r'\s*\([^)]*\)', '', x))
SCOPUS['normalized_name'] = SCOPUS['name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower().apply(lambda x: re.sub(r'\s*\([^)]*\)', '', x))

SCOPUS['normalized_variants'] = SCOPUS['normalized_variants'].apply(ast.literal_eval)

def match_institutions(inst_EMEC, inst_SCOPUS):
    inst_EMEC = inst_EMEC.reset_index().rename(columns={'index': 'A_original_index'})
    inst_SCOPUS = inst_SCOPUS.reset_index().rename(columns={'index': 'B_original_index'})

    match1 = pd.merge(
        inst_EMEC.dropna(subset=['normalized_Instituição(IES)']),
        inst_SCOPUS.dropna(subset=['normalized_name']),
        left_on='normalized_Instituição(IES)',
        right_on='normalized_name',
        suffixes=('_A', '_B')
    )
    match1['name_match'] = 1


    match2 = pd.merge(
        inst_EMEC.dropna(subset=['normalized_Sitio']),
        inst_SCOPUS.dropna(subset=['normalized_url']),
        left_on='normalized_Sitio',
        right_on='normalized_url',
        suffixes=('_A', '_B')
    )
    match2['url_match'] = 1

    match3 = pd.merge(
        inst_EMEC.dropna(subset=['normalized_Sitio']),
        inst_SCOPUS.dropna(subset=['normalized_domain']),
        left_on='normalized_Sitio',
        right_on='normalized_domain',
        suffixes=('_A', '_B')
    )
    match3['domain_match'] = 1
    
    inst_EMEC_filtered = inst_EMEC.dropna(subset=['normalized_Instituição(IES)'])
    inst_SCOPUS_filtered = inst_SCOPUS.dropna(subset=['normalized_variants'])
    
    inst_SCOPUS_exploded = inst_SCOPUS_filtered.explode('normalized_variants')
    inst_SCOPUS_exploded = inst_SCOPUS_exploded.dropna(subset=['normalized_variants'])
    
    match4 = pd.merge(
        inst_EMEC_filtered,
        inst_SCOPUS_exploded,
        left_on='normalized_Instituição(IES)',
        right_on='normalized_variants',
        suffixes=('_A', '_B')
    )
    match4['variant_match'] = 1

    all_matches = pd.concat([match1, match2, match3, match4], ignore_index=True)
    
    agg_dict = {col: 'first' for col in all_matches.columns if col not in ['A_original_index', 'B_original_index']}
    agg_dict.update({
        'name_match': 'sum',
        'url_match': 'sum',
        'domain_match': 'sum',
        'variant_match': 'sum'
    })
    
    final_matches = all_matches.groupby(['A_original_index', 'B_original_index']).agg(agg_dict).reset_index()
    flag_columns = ['name_match', 'url_match', 'domain_match', 'variant_match']
    final_matches[flag_columns] = final_matches[flag_columns].fillna(0).astype(int)
    
    return final_matches

def count_matches_for_b(source_b_df, matches_df):
    match_counts = matches_df['B_original_index'].value_counts().reset_index()
    match_counts.columns = ['B_original_index', 'match_count']

    source_b_df = source_b_df.reset_index().rename(columns={'index': 'B_original_index'})
    
    result_df = pd.merge(source_b_df, match_counts, on='B_original_index', how='left')
    result_df['match_count'] = result_df['match_count'].fillna(0).astype(int)
    result_df = result_df.drop(columns='B_original_index')

    return result_df

matched_df = match_institutions(EMEC, SCOPUS)
institutions_b_with_counts = count_matches_for_b(SCOPUS, matched_df)

matched_df.drop(['A_original_index', 'normalized_Instituição(IES)', 'level_0', 'B_original_index', 'normalized_variants', 'normalized_name', 'normalized_domain', 'normalized_url', 'normalized_Sitio'], axis=1, inplace=True)
institutions_b_with_counts.drop(['level_0', 'normalized_variants', 'normalized_name', 'normalized_domain', 'normalized_url'], axis=1, inplace=True)

matched_df.to_csv('./data/code/matchDuplicatedNames/matchScopusToEMEC/matches.csv', index=False)
institutions_b_with_counts.to_csv('./data/code/matchDuplicatedNames/matchScopusToEMEC/counting.csv', index=False)