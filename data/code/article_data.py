from pybliometrics.scopus import AbstractRetrieval
from pybliometrics.scopus import init as ScopusInit
import pandas as pd
import json
import csv
import multiprocessing

ScopusInit()

STAGING_DIR = './data/data/staging'
FIELDNAMES = {'article': ['eid', 'title', 'published date', 'cited by count'],
                'article subject areas': ['eid', 'subject area code'],
                'articles author': ['eid', 'auid', 'creator'],
                'author': ['auid', 'given name', 'surname', 'indexed name'],
                'authors institution': ['affiliation id', 'auid', 'dptid', 'organization'],
                'institution': ['affiliation id', 'country', 'city']
                }
with open(f'./data/code/subject_areas_abbreviation.json', 'r') as subjareas_file:
    SUBJECT_AREAS = json.load(subjareas_file)['subject_areas']
    

def write_data(data, data_type, lock):
    with lock:
        filename = f'{STAGING_DIR}/{data_type}.csv' if data_type != 'institution' else f'./data/data/raw/{data_type}.csv'
        with open(filename, 'a+', encoding="utf-8", newline='') as articles:
            writer = csv.DictWriter(articles, fieldnames=FIELDNAMES.get(data_type))
            writer.writerows(data)


def worker_function(year, lock):
    article_data = []
    article_subjareas = []
    articles_author = []
    author = []
    authors_institution = []
    institution = []
    institution_ids = set()
    try:
        for area in SUBJECT_AREAS:
            df = pd.read_csv(f'./data/data/raw/scopus/{year}/{area}_{year}.csv')
            eids = df['eid'].tolist()
            eids_count = len(eids)
            
            for (index, eid) in enumerate(eids):
                print(f'({index+1}/{eids_count}) Fetching data for year: {year}, subject area: {area}, EID: {eid}')
                article = AbstractRetrieval(eid, view='FULL', verbose=True)
                
                article_data.append({'eid': article.eid, 
                                     'title': article.title, 
                                     'published date': article.coverDate, 
                                     'cited by count': article.citedby_count})
                for subarea in article.subject_areas:
                    article_subjareas.append({'eid': article.eid, 
                                              'subject area code': subarea.code})
                    
                for authors in article.authors:
                    creator = authors.indexed_name == df.iloc[index]['creator']
                    if authors.auid is not None:
                        author.append({'auid': authors.auid, 
                                   'given name': authors.given_name, 
                                   'surname': authors.surname, 
                                   'indexed name': authors.indexed_name})
                        articles_author.append({'eid': article.eid, 
                                            'auid': authors.auid, 
                                            'creator': creator})
                    
                for authorgroup in article.authorgroup:
                    if authorgroup.affiliation_id is not None:               
                        authors_institution.append({'affiliation id': authorgroup.affiliation_id, 
                                                    'auid': authorgroup.auid, 
                                                    'dptid': authorgroup.dptid, 
                                                    'organization': authorgroup.organization})
                        if authorgroup.affiliation_id not in institution_ids:
                            institution.append({'affiliation id': authorgroup.affiliation_id, 
                                                    'country': authorgroup.country, 
                                                    'city': authorgroup.city})
                        institution_ids.add(authorgroup.affiliation_id)
            
            print(f'--- Completed fetching data for subject area: {area} ---')
        print(f'--- Completed fetching data for year: {year} ---')
        
    except Exception as e:
        print(e)
    finally:
        write_data(article_data, 'article', lock)
        write_data(article_subjareas, 'article subject areas', lock)
        write_data(author, 'author', lock)
        write_data(authors_institution, 'authors institution', lock)
        write_data(articles_author, 'articles author', lock)
        write_data(institution, 'institution', lock)
        

if __name__ == '__main__':
    for data_type in ['article', 'article subject areas', 'author', 'authors institution', 'articles author', 'institution']:
        filename = f'{STAGING_DIR}/{data_type}.csv' if data_type != 'institution' else f'./data/data/raw/{data_type}.csv'
        with open(filename, 'w', encoding="utf-8", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES.get(data_type))
            writer.writeheader()


    with multiprocessing.Manager() as manager:
        lock = manager.Lock()
        with multiprocessing.Pool() as pool:
            pool.starmap(worker_function, [(year, lock) for year in range(2015, 2025)])