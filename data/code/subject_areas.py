import json
import csv

STAGING_DIR = './data/data/staging'

with open('./data/data/raw/article_subareas.json', 'r') as subject_areas_file:
    all_areas = json.load(subject_areas_file)
    subjareas = []
    for area in all_areas:
        abbreviation = area['desc'][:4].upper()
        if area['code'] == 15:
            abbreviation = 'CENG'
        for subarea in area['child']:
            subjareas.append({'code': subarea['code'], 'area': subarea['desc'], 'abbreviation': abbreviation})
            
with open(f'{STAGING_DIR}/subject area.csv', 'w', encoding="utf-8", newline='') as articles:           
    writer = csv.DictWriter(articles, fieldnames=['code', 'area', 'abbreviation'])
    writer.writerows(subjareas)