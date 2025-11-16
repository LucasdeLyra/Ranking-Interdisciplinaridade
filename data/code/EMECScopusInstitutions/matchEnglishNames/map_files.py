from collections import defaultdict
import pandas as pd
import json
import re
import ast

def normalize_site(url):
    if pd.isna(url):
        return url
        
    url = str(url).lower().strip()
    url = url.replace('http://', '')
    url = url.replace('https://', '') 
    url = url.replace('www.', '')
    url = url.replace('www2.', '')
    url = url.replace('www3.', '')
    url = url.replace('www4.', '')
    url = url.replace('www5.', '')
    if url.endswith('/'):
        url = url.rstrip('/')
    return url

def normalize_name(text):
    if pd.isna(text):
        return text
    return str(text).normalize('NFKD').encode('ascii', errors='ignore').decode('utf-8').lower()

# Load and prepare data
SCOPUS = pd.read_csv('./data/code/EMECScopusInstitutions/fullMatch/counting.csv', encoding='utf-8')
SCOPUS = SCOPUS[SCOPUS['match_count'] == 0].copy()

with open('./data/code/EMECScopusInstitutions/matchEnglishNames/ror_brazil.json', 'r', encoding='utf-8') as f:
    ROR_DATA = json.load(f)

# Convert ROR data to DataFrame
ror_rows = []
for inst in ROR_DATA:
    if not inst:  # Skip empty entries  
        continue
    row = {
        'ror_id': inst.get('id', ''),
        'name': inst.get('name', ''),
        'links': inst.get('links', []),
        'labels': inst.get('labels', []),
        'relationships': inst.get('relationships', [])
    }
    ror_rows.append(row)

ROR = pd.DataFrame(ror_rows)

# Prepare SCOPUS data
SCOPUS['normalized_domain'] = SCOPUS['domain'].apply(normalize_site)
SCOPUS['normalized_url'] = SCOPUS['url'].apply(normalize_site)
SCOPUS['normalized_name'] = SCOPUS['name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower().apply(lambda x: re.sub(r'\s*\([^)]*\)', '', x))
SCOPUS['normalized_variants'] = SCOPUS['variants'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower().apply(lambda x: re.sub(r'\s*\([^)]*\)', '', x))

def normalize_label(label_list):
    if not isinstance(label_list, list):
        return []
    normalized_labels = []
    for label in label_list:
        if isinstance(label, str):
            normalized = label.normalize('NFKD').encode('ascii', errors='ignore').decode('utf-8').lower()
            normalized = re.sub(r'\s*\([^)]*\)', '', normalized)
            normalized_labels.append(normalized)
    return normalized_labels

ROR['normalized_name'] = ROR['name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower().apply(lambda x: re.sub(r'\s*\([^)]*\)', '', x))
ROR['normalized_labels'] = ROR['labels'].apply(normalize_label)
ROR['normalized_links'] = ROR['links'].apply(normalize_site)


def match_with_ror_data():
    # Explode normalized_links for domain/url matching
    ror_links = ROR.explode('normalized_links').dropna(subset=['normalized_links'])
    
    # Match by domain
    domain_matches = pd.merge(
        SCOPUS.dropna(subset=['normalized_domain']),
        ror_links,
        left_on='normalized_domain',
        right_on='normalized_links',
        how='inner'
    )
    domain_matches['match_type'] = 'domain'
    
    # Match by URL
    url_matches = pd.merge(
        SCOPUS.dropna(subset=['normalized_url']),
        ror_links,
        left_on='normalized_url',
        right_on='normalized_links',
        how='inner'
    )
    url_matches['match_type'] = 'url'
    
    # Match by name
    name_matches = pd.merge(
        SCOPUS.dropna(subset=['normalized_name']),
        ROR,
        left_on='normalized_name',
        right_on='normalized_name',
        how='inner'
    )
    name_matches['match_type'] = 'name'
    
    # Explode variants for matching
    scopus_variants = SCOPUS.explode('normalized_variants').dropna(subset=['normalized_variants'])
    ror_labels = ROR.explode('normalized_labels').dropna(subset=['normalized_labels'])
    
    # Match variants with names and labels
    variant_matches = pd.merge(
        scopus_variants,
        ror_labels,
        left_on='normalized_variants',
        right_on='normalized_labels',
        how='inner'
    )
    variant_matches['match_type'] = 'variant'
    
    # Combine all matches
    all_matches = pd.concat([domain_matches, url_matches, name_matches, variant_matches], ignore_index=True)
    
    # Process hospital matches
    for idx, row in all_matches.iterrows():
        if 'hospital' in row['name_x'].lower():
            # Find the corresponding ROR data
            ror_data = ROR_DATA[ROR[ROR['ror_id'] == row['ror_id']].index[0]]
            if ror_data:
                # Check for university relationship
                univ_rel = find_university_relation(ror_data)
                if univ_rel:
                    # Store both hospital and university information
                    all_matches.at[idx, 'hospital_name'] = row['name_y']
                    all_matches.at[idx, 'hospital_id'] = row['ror_id']
                    all_matches.at[idx, 'ror_id'] = univ_rel[0]
                    all_matches.at[idx, 'name_y'] = univ_rel[1]
    
    # Select and rename columns for output
    result_columns = {
        'eid': 'eid',
        'match_type': 'match_type',
        'name_x': 'scopus_name',
        'name_y': 'ror_name',
        'ror_id': 'ror_id',
        'hospital_name': 'hospital_name',
        'hospital_id': 'hospital_id'
    }
    
    final_matches = all_matches[list(result_columns.keys())].rename(columns=result_columns)
    final_matches = final_matches.drop_duplicates(subset=['eid'])
    
    return final_matches

def main():
    matches_df = match_with_ror_data()
    matches_df.to_csv('./data/code/EMECScopusInstitutions/matchEnglishNames/ror_matches.csv', index=False)
    print(f"Found {len(matches_df)} matches using ROR data")
    print("Matches saved to ror_matches.csv")

if __name__ == "__main__":
    main()
