from pybliometrics.scopus import ScopusSearch
from pybliometrics.scopus import init as ScopusInit

import pandas as pd
import json

ScopusInit()  # read API keys

with open(f'./data/code/subject_areas_abbreviation.json', 'r') as subjareas_file:
    SUBJECT_AREAS = json.load(subjareas_file)['subject_areas']
    
for year in range(2015, 2025):
    for area in SUBJECT_AREAS:
        query = f'PUBYEAR = {year} AND AFFILCOUNTRY ( Brazil ) AND SUBJAREA ( {area} )'
        search = ScopusSearch(query, view='STANDARD', verbose=True)
        df = pd.DataFrame(search.results)
        output_file = f'./data/data/raw/{year}/{area}_{year}.csv'

        df.to_csv(output_file, index=False)
        print(f'Saved results to {output_file}')
        