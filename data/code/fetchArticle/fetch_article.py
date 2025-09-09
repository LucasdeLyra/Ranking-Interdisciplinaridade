from pybliometrics.scopus import AbstractRetrieval
from pybliometrics.scopus import init as ScopusInit
from sys import argv
from time import sleep
from collections import namedtuple
import pandas as pd
import json
import csv
import threading
import concurrent.futures
import os
import queue
import itertools

ScopusInit()
WorkItem =  namedtuple('WorkItem', ['year', 'subject_area', 'work_done_queue', 'error_queue'])
STAGING_DIR = './data/data/staging'
CHECKPOINT_PATH = './data/code/fetchArticle/CHECKPOINT'
FIELDNAMES = {'article': ['eid', 'title', 'published date', 'cited by count'],
              'article_subject_areas': ['eid', 'subject area code'],
              'articles_author': ['eid', 'auid', 'creator'],
              'author': ['auid', 'given name', 'surname', 'indexed name'],
              'authors_institution': ['affiliation id', 'auid', 'dptid', 'organization'],
              'institution': ['affiliation id', 'country', 'city']
            }
with open(f'./data/code/subject_areas_abbreviation.json', 'r') as subjareas_file:
    SUBJECT_AREAS = json.load(subjareas_file)['subject_areas']

def load_work_done():
    if not os.path.exists(CHECKPOINT_PATH):
        return {}
    try:
        with open(CHECKPOINT_PATH, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}

def writer(checkpoint, work_done, error):
    while True:
        if not error.empty():
            error_item = error.get()
            if error_item is None:
                break
            
            with open('./data/code/fetchArticle/ERRORS', 'a+') as error_log:
                error_log.write(f'{error_item}\n')
        
        if not work_done.empty():
            processed_item = work_done.get()
            if processed_item is None:
                break

            year, area, index, data = processed_item
            if write_data(data, year):
                if str(year) not in checkpoint:
                    checkpoint[str(year)] = {}
                if area not in checkpoint[str(year)]:
                    checkpoint[str(year)][area] = 0
                
                checkpoint[str(year)][area] = index
                
                with open(CHECKPOINT_PATH, 'w+') as f:
                    json.dump(checkpoint, f, indent = 4)
            else:
                error.put((f'CRITICAL ERROR: Writing data failed for {year} : {area}\n'))
                print(f'Error writing data for {year} : {area}')
                

def write_data(files, year):
    for data_type, data in files.items():
        filename = f'{STAGING_DIR}/{year}/{data_type}.csv' if data_type != 'institution' else f'./data/data/raw/{data_type}.csv'
        with open(filename, 'a+', encoding='utf-8', newline='') as articles:
            csvWriter = csv.DictWriter(articles, fieldnames=FIELDNAMES.get(data_type))
            csvWriter.writerows(data)
        
    return True

def get_article_info(article):
    return {'eid': article.eid, 
            'title': article.title, 
            'published date': article.coverDate, 
            'cited by count': article.citedby_count}

def get_subject_areas(article):
    subject_area = []
    for subarea in article.subject_areas:
        subject_area.append({'eid': article.eid, 
                              'subject area code': subarea.code})
    return subject_area

def get_authors(article, creator_name):
    author = []
    articles_author = []
    for authors in article.authors:
        is_creator = authors.indexed_name == creator_name
        if authors.auid is not None:
            author.append({'auid': authors.auid, 
                           'given name': authors.given_name, 
                           'surname': authors.surname, 
                           'indexed name': authors.indexed_name})
            articles_author.append({'eid': article.eid, 
                                    'auid': authors.auid, 
                                    'creator': is_creator})
    return author, articles_author

def get_affiliations(article, institution_ids):
    authors_institution = []
    institution = []
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
            
    return authors_institution, institution, institution_ids

def process_already_done(year, area, checkpoint, length):
    if str(year) in checkpoint and area in checkpoint[str(year)]:
        if checkpoint[str(year)][area] == length - 1:
            return length
        if checkpoint[str(year)][area] > length - 1:
            return 0 #Last process error, better proccess everything again
        return checkpoint[str(year)][area]
    return 0

def fetch_article_data(work_item):
    year, area, work_done, error = work_item
    files = {key: [] for key in FIELDNAMES}
    institution_ids = set()
    
    df = pd.read_csv(f'./data/data/raw/scopus/{year}/{area}_{year}.csv')
    df_length = len(df)
    
    already_done = process_already_done(year, area, load_work_done(), df_length)
    df_length = len(df.iloc[already_done:])
    for index, row in df.iloc[already_done:].iterrows():
        try:
            eid = row['eid']
            creator_name = row['creator']
            article = AbstractRetrieval(eid, view='FULL', verbose=True)
            
            files['article'].append(get_article_info(article))
            files['article_subject_areas'].extend(get_subject_areas(article))
            
            aux_auth, aux_article_auth = get_authors(article, creator_name)
            files['articles_author'].extend(aux_article_auth)
            files['author'].extend(aux_auth)
            
            aux_auth_inst, aux_inst, aux_inst_ids = get_affiliations(article, institution_ids)
            files['authors_institution'].extend(aux_auth_inst)
            files['institution'].extend(aux_inst)
            institution_ids = aux_inst_ids
            
            if index % 1000 == 0 or (index + 1) == df_length:
                work_done.put((year, area, already_done+index, files))
                print(f'({already_done+index+1:05d}/{df_length:05d}) Fetching data for {year} : {area} : {eid}')
                files = {key: [] for key in FIELDNAMES}

        except Exception:
            error.put((eid))
            print(f'Error fetching data for {year} : {area} : {eid}')
            sleep(2)
            
    print(f'--- Completed fetching data: {year} : {area} ---')


if __name__ == '__main__':
    YEARS = list(range(2015, 2025))
    work_done = queue.Queue()
    error = queue.Queue()
    WORK_ITEM = [WorkItem(*item) for item in itertools.product(YEARS, SUBJECT_AREAS, [work_done], [error])]
    
    checkpoint = load_work_done()
    if not checkpoint:
        for data_type in FIELDNAMES.keys():
            for year in YEARS:
                filename = f'{STAGING_DIR}/{year}/{data_type}.csv' if data_type != 'institution' else f'./data/data/raw/{data_type}.csv'
                with open(filename, 'w+', encoding='utf-8', newline='') as f:
                    csvWriter = csv.DictWriter(f, fieldnames=FIELDNAMES.get(data_type))
                    csvWriter.writeheader()
                
    writer_thread = threading.Thread(target=writer, args=(checkpoint, work_done, error))
    writer_thread.start()
    
    if len(argv) > 1 and argv[1] == 'single':
        for item in WORK_ITEM:
            fetch_article_data(WORK_ITEM[3])
    else:   
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            executor.map(fetch_article_data, WORK_ITEM)
        
        work_done.put(None)
        error.put(None)
        writer_thread.join()
