import os
import time

import pandas as pd

print('Reading files...')
articles_raw = pd.read_parquet('../data/data/refined/parquets/article.parquet').rename(columns={
    'eid': 'art_id',
    'subtype description': 'art_subtype',
    'title': 'art_title',
    'published date': 'art_published_date',
    'cited by count': 'art_citations',
    'source id' : 'art_source',
    'aggregation type' : 'art_source_type'
})[['art_id', 'art_subtype', 'art_title', 'art_published_date', 'art_citations', 'art_source', 'art_source_type']] # B

scores_raw = pd.read_parquet('../data/data/refined/parquets/citescore.parquet').rename(columns={
    'Scopus Source ID' : 'art_source',
    'Title' : 'src_title',
    'Citation Count' : 'src_citation_count',
    'CiteScore' : 'src_citescore',
    'Scopus Sub-Subject Area' : 'src_subject_area',
    'Percentile' : 'src_percentile',
    'RANK' : 'src_rank',
    'Rank Out Of' : 'src_rank_count',
    'Quartile' : 'src_quartile'
})[['art_source', 'src_title', 'src_citation_count', 'src_citescore', 'src_subject_area', 'src_percentile', 'src_rank', 'src_rank_count', 'src_quartile']]

authors_institutions_raw = pd.read_parquet('../data/data/refined/parquets/authors_institution.parquet').rename(columns={
    #eid,auid,creator,afid,dptid,organization,country,city
    'eid': 'art_id',
    'auid': 'aut_id',
    'creator': 'main_writer',
    'afid': 'ins_id',
    'dptid': 'dpt_id',
    'organization': 'ins_org',
    'country': 'ins_country',
    'city': 'ins_city'
})

institutions_raw = (pd.read_parquet('../data/data/refined/parquets/institutions.parquet').rename(columns={
        'Código Mantenedora': 'ins_maint_id',
        'Categoria': 'ins_category',
        'afid': 'ins_id',
        'Situação da IES': 'ins_status',
        'Código IES': 'ins_code',
        'Instituição(IES)': 'ins_name'
    })
    [['ins_maint_id', 'ins_category', 'ins_id', 'ins_status', 'ins_code', 'ins_name']]
)

authors_raw = pd.read_parquet('../data/data/refined/parquets/author.parquet').rename(columns={
    'auid': 'aut_id',
    'given name': 'aut_name',
    'surname': 'aut_surname',
    'indexed name': 'aut_indexed_name'
})[['aut_id', 'aut_name', 'aut_surname', 'aut_indexed_name']]

subject_areas_raw = pd.read_parquet('../data/data/refined/parquets/subject_area.parquet')
subject_areas_raw.columns = ['sub_id', 'src_subject_area', 'src_subject_macro']


print('Merging data...')
main = (
articles_raw
    .merge(scores_raw, on='art_source', how='inner')
    .merge(authors_institutions_raw, on='art_id', how='inner')
    .merge(institutions_raw, on='ins_id', how='inner')
    .merge(authors_raw, on='aut_id', how='inner')
    .merge(subject_areas_raw, on='src_subject_area', how='inner')
)

print('Data merged successfully!')
ranking = main[['art_id', 'art_subtype', 'art_published_date', 'src_subject_area', 'src_subject_macro', 'src_quartile', 'ins_maint_id', 'ins_name', 'ins_category', 'ins_country', 'aut_id', 'src_percentile', 'ins_code']]
institution = main[['ins_maint_id', 'ins_name', 'ins_category', 'ins_status', 'ins_code']].drop_duplicates(subset=['ins_maint_id'])

def get_score_by_quartile(quartile: int, ranking: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate interdisciplinarity (DI) values for articles filtered by quartile.
    
    This function filters articles and computes a DI_value based on the number of authors
    and optionally the quartile of the publication source. The DI_value represents the
    contribution of each article to institutional interdisciplinarity, normalized by
    the number of authors.
    
    Parameters:
    -----------
    quartile : int
        Quartile filter for source publications:
        - -3: No weighting (all articles counted with DI_value = 1)
        - -2: Use source percentile for weighting (all articles counted with percentile-based DI calculation)
        - -1: No quartiles used in the calculation
        - 0: No quartile filter (all articles counted with base DI calculation)
        - 1, 2, 3, 4: Only articles from sources in specified quartile
    
    ranking : pd.DataFrame
        DataFrame to be filtered
    
    Returns:
    --------
    pd.DataFrame
        Modified ranking DataFrame with added 'DI_value' column containing:
        - quartile == -2: DI_value = 10 / (number_of_authors * number_of_areas * all_percentile)
        - quartile == -1: DI_value = 10 / (number_of_authors * number_of_areas)
        - quartile == 0: DI_value = 10 / (number_of_authors * number_of_areas * all_quartile)
        - quartile in [1,2,3,4]: DI_value = 10 / (number_of_authors * number_of_areas * specified_quartile)
    
    Notes:
    ------
    - Only articles (art_subtype == 'Article') are included
    - DI_value is normalized by dividing by the number of authors per article
    - Higher quartile values (top journals) result in lower DI_value when quartile != 0
    - This weighting accounts for institutional credit distributed across multi-author papers
    """
    ranking_article = ranking[(ranking['art_subtype'] == 'Article')]
    
    if quartile == -3:
        ranking_article['DI_value'] = 1
        ranking_article = ranking_article.drop(['src_quartile', 'src_percentile'], axis=1)
    elif quartile == -2:
        ranking_article['DI_value'] = (1/(ranking_article.groupby('art_id')['aut_id'].transform('count')))*ranking_article['src_percentile']
        ranking_article = ranking_article.drop(['src_quartile'], axis=1)
    elif quartile == -1:
        ranking_article['DI_value'] = (1/(ranking_article.groupby('art_id')['aut_id'].transform('count')))*10
        ranking_article = ranking_article.drop(['src_percentile'], axis=1)
    elif quartile == 0:
        ranking_article['DI_value'] = (1/(ranking_article.groupby('art_id')['aut_id'].transform('count') * ranking_article['src_quartile']))*10
        ranking_article = ranking_article.drop(['src_percentile'], axis=1)
    else:
        ranking_article = ranking_article[(ranking_article['src_quartile'] == quartile)]
        ranking_article['DI_value'] = (1/(ranking_article.groupby('art_id')['aut_id'].transform('count') * ranking_article['src_quartile']))*10
        ranking_article = ranking_article.drop(['src_percentile'], axis=1)
    return ranking_article

def calculate_di_tables(ranking_article: pd.DataFrame, institution: pd.DataFrame, cutoff: float = 0) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calculate DI (Interdisciplinarity) tables at both subject and macro levels.
    
    Parameters:
    -----------
    ranking_article : pd.DataFrame
        DataFrame containing article data with DI_value column
    institution : pd.DataFrame
        DataFrame containing institution information with ins_maint_id
    cutoff : float
        If cutoff > 0, removes items where DI_value is less than cutoff
    
    Returns:
    --------
    tuple of (di_table_subs, di_table_macros)
        - di_table_subs: DI table at subject area level with only ins_name and subject columns
        - di_table_macros: DI table at macro area level with only ins_name and macro columns
    """

    sub_sums = ranking_article.copy()
    macro_sums = ranking_article.copy()

    sub_sums = sub_sums.groupby(['ins_code', 'src_subject_area'])['DI_value'].sum()
    if cutoff > 0:
        sub_sums = sub_sums[sub_sums >= cutoff]
    pivoted_area = sub_sums.unstack(fill_value=0).reset_index()

    di_table_subs = (
        institution
        .merge(pivoted_area, on='ins_code', how='left')
        .fillna(0)
    )

    macro_sums = macro_sums.groupby(['ins_code', 'src_subject_macro'])['DI_value'].sum()
    if cutoff > 0:
        macro_sums = macro_sums[macro_sums >= cutoff]
    pivoted_macro = macro_sums.unstack(fill_value=0).reset_index()

    di_table_macros = (
        institution
        .merge(pivoted_macro, on='ins_code', how='left')
        .fillna(0)
    )

    di_table_subs = di_table_subs.drop(['ins_code', 'ins_category', 'ins_status'], axis=1)
    di_table_macros = di_table_macros.drop(['ins_maint_id', 'ins_category', 'ins_status', 'ins_code'], axis=1)
    return di_table_subs, di_table_macros

def order_by_non_zero_count(df: pd.DataFrame, path: str) -> pd.DataFrame:
    """
    Orders DataFrame rows by the number of non-zero values in ascending order (fewest zeros first).
    
    Parameters:
    -----------
    df : pd.DataFrame
        Input dataframe to be sorted
    path : str
        Directory path where to save the zeros count file
    
    Returns:
    --------
    pd.DataFrame
        DataFrame sorted by non-zero count (rows with more non-zero values come first)
    """
    non_zero_counts = (df != 0).sum(axis=1)
    zeros_count = len(df.columns) - non_zero_counts - 1  # -1 for ins_name column
    
    # Save zeros count to file
    zeros_df = pd.DataFrame({
        'ins_name': df.iloc[:, 0],
        'zeros_count': zeros_count.values
    })
    zeros_df = zeros_df.iloc[non_zero_counts.argsort()[::-1]]
    zeros_df.to_csv(f'{path}/zeros_count.csv', index=False)
    
    # Sort by non-zero count in descending order (most non-zero values first)
    return df.iloc[non_zero_counts.argsort()[::-1]]

def get_output_paths(quartile: int, cutoff: float = 0) -> tuple:
    """
    Generate output directory paths based on quartile value.
    
    Parameters:
    -----------
    quartile : int
        Quartile filter value:
        - -3: Returns paths for 'sensitivity' directories
        - -2: Returns paths for 'percentile' directories
        - -1: Returns paths for 'no_quality' directories
        - 0: Returns paths for 'quartile' directories
        - 1, 2, 3, 4: Returns paths for 'q{i}' directories
    
    Returns:
    --------
    tuple of (macro_path, sub_path)
        Paths for saving macro and subject level ranking files
    """
    
    macro_path = './data/macro/macro_'
    sub_path = './data/sub/sub_'

    if cutoff > 0:
        macro_path = './data/sensitivity/macro/macro_'
        sub_path = './data/sensitivity/sub/sub_'
    
    if quartile == -3:
        macro_path += 'no_average'
        sub_path += 'no_average'
    elif quartile == -2:
        macro_path += 'percentile'
        sub_path += 'percentile'
    elif quartile == -1:
        macro_path += 'no_quality'
        sub_path += 'no_quality'
    elif quartile == 0:
        macro_path += 'quartile'
        sub_path += 'quartile'
    else:
        macro_path += f'q{quartile}'
        sub_path += f'q{quartile}'
    
    if cutoff > 0:
        macro_path += f'_cutoff_{cutoff}'
        sub_path += f'_cutoff_{cutoff}'
    
    return macro_path, sub_path


def save_ranking_tables_with_path(di_table_subs: pd.DataFrame, di_table_macros: pd.DataFrame, 
                                   macro_path: str, sub_path: str, top_n: int = 200):
    """
    Save ranking tables at macro and subject levels to specified paths.
    
    Parameters:
    -----------
    di_table_subs : pd.DataFrame
        DI table at subject area level with ins_name and subject area columns
    di_table_macros : pd.DataFrame
        DI table at macro area level with ins_name and macro area columns
    macro_path : str
        Base directory path for macro-level output files
    sub_path : str
        Base directory path for subject-level output files
    top_n : int, optional
        Number of top institutions to retain (default: 200)
    
    Returns:
    --------
    None
        Files are saved to disk in the specified directories
    """
    
    # Create directories if they don't exist
    os.makedirs(macro_path, exist_ok=True)
    os.makedirs(sub_path, exist_ok=True)
    
    # Process macro-level rankings
    final_leys_macros = order_by_non_zero_count(di_table_macros, macro_path)[:top_n]
    final_leys_macros['ins_name'].to_csv(f'{macro_path}/institutions_names.csv', index=False)
    final_leys_macros = final_leys_macros.drop(columns=['ins_name'])
    final_leys_macros.columns.to_frame(index=False).to_csv(f'{macro_path}/labels.csv', index=False, header=False)
    final_leys_macros.T.to_csv(f'{macro_path}/rank.txt', index=False, header=False)

    # Process subject-level rankings
    final_leys_subs = order_by_non_zero_count(di_table_subs, sub_path)[:top_n]
    final_leys_subs['ins_name'].to_csv(f'{sub_path}/institutions_names.csv', index=False)
    final_leys_subs = final_leys_subs.drop(columns=['ins_name'])
    final_leys_subs.columns.to_frame(index=False).to_csv(f'{sub_path}/labels.csv', index=False, header=False)
    final_leys_subs.T.to_csv(f'{sub_path}/rank.txt', index=False, header=False)

# Execute for all quartile values
cutoffs = [0, 0.1, 0.2, 0.5]
for cutoff in cutoffs:
    print(f"\nProcessing quartiles for cutoff {cutoff}...")
    for quartile in range(-3, 5):
        print(f"\tProcessing quartile: {quartile}")
        pd.set_option('display.max_columns', None)

        # Calculate DI values for this quartile
        ranking_article = get_score_by_quartile(quartile, ranking)
        #ranking_article.to_csv(f'./data/debug/ranking_article_quartile_{quartile}.csv', index=False)  # Save intermediate ranking_article for debugging
        
        # Calculate DI tables
        cutoff_limiar = cutoff
        if (quartile == -3) and (cutoff > 0):
            cutoff_limiar = cutoff_limiar*10
        di_table_subs, di_table_macros = calculate_di_tables(ranking_article, institution, cutoff_limiar)

        # Get output paths
        macro_path, sub_path = get_output_paths(quartile, cutoff_limiar)
        
        di_table_subs.to_csv(f'.debug/{sub_path}', index=False)  # Save intermediate di_table_subs for debugging
        di_table_macros.to_csv(f'./debug/{macro_path}', index=False)  # Save intermediate di_table_macros for debugging
        
        # Save ranking tables to appropriate paths
        save_ranking_tables_with_path(di_table_subs, di_table_macros, macro_path, sub_path, top_n=200)
        print(f"\tSaved to: {macro_path} and {sub_path}")
    print(f"All quartiles for cutoff {cutoff} processed successfully!")
print(f"\nFinished!")


