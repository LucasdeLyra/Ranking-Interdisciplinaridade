from pybliometrics.scopus import AffiliationRetrieval
from pybliometrics.scopus import init as ScopusInit
import pandas as pd
from collections import defaultdict

ScopusInit()

df = pd.read_csv(f'./data/data/raw/institution.csv', dtype=object)
df.drop_duplicates(subset=['afid'], inplace=True)
df = df.loc[df['country'].isin(['Brazil', 'Brasil'])]
length = len(df)

affiliations_data = []
a = 0
for i, row in df.iterrows():
    if i >= 10000:
        break
    try:
        affiliation = AffiliationRetrieval(row.afid)
        data = {
            "index": a,
            "afid": row.afid,
            "name": affiliation.affiliation_name,
            "domain": affiliation.org_domain,
            "url": affiliation.org_URL,
            "variants": [v.name for v in (affiliation.name_variants or [])]
        }
        affiliations_data.append(data)

    except Exception as e:
        print(f'Error retrieving affiliation {row.afid}: {e}')
    
    finally:
        a += 1

df_save = pd.DataFrame(affiliations_data, columns=["index", "afid", "name", "domain", "url", "variants"])
df_save.to_csv(f'./example.csv', index=False)

name_map = defaultdict(set)
domain_map = defaultdict(set)
url_map = defaultdict(set)
variant_map = defaultdict(set)

for aff in affiliations_data:
    if aff["name"]:
        name_map[aff["name"].lower()].add(aff["index"])
    if aff["domain"]:
        domain_map[aff["domain"].lower()].add(aff["index"])
    if aff["url"]:
        url_map[aff["url"].lower()].add(aff["index"])
    for v in aff["variants"]:
        variant_map[v.lower()].add(aff["index"])

old_name_map = name_map.copy()  
i = False
# Union by matching fields
for aff in affiliations_data:
    idx = aff["index"]

    # Same name
    if aff["name"] and aff["name"].lower() in name_map:
        name_map[aff["name"].lower()].add(idx)
        #print('Found association with same name:', aff["name"])
        i = True

    # Same domain
    if aff["domain"] and aff["domain"].lower() in domain_map:
        name_map[aff["name"].lower()].add(idx)
        #print('Found association with same domain:', aff["domain"])
        i = True


    # Same URL
    if aff["url"] and aff["url"].lower() in url_map:
        name_map[aff["name"].lower()].add(idx)
        #print('Found association with same URL:', aff["url"])
        i = True
        

    # Name in another's variants
    for variant in aff["variants"]:
        v = variant.lower()
        if v in name_map:
            name_map[aff["name"].lower()].add(idx)
            #print('Found association with name in variant:', variant)
            i = True

    # Variant in another's variants
    for variant in aff["variants"]:
        v = variant.lower()
        if v in variant_map:
            name_map[aff["name"].lower()].add(idx)
            #print('Found association with variant in variant:', variant)
            i = True
    
print(old_name_map == name_map)   

print(old_name_map)
print('--------------')
print(name_map)
"""# Assign group IDs
group_id_map = {}
current_group_id = 0
group_ids = []

for i in range(length):
    root = dus.find(i)
    if root not in group_id_map:
        group_id_map[root] = current_group_id
        current_group_id += 1
    group_ids.append(group_id_map[root])

# Add to dataframe
df["institution_group_id"] = group_ids

# Save results
df.to_csv("./institution_groups.csv", index=False)
print("Grouping completed and saved.")"""