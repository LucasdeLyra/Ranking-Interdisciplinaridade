import pandas as pd
import os

# Define file paths
scopus_emec_domain_path = "./data/code/matchDuplicatedNames/0_matchScopusToEMECByDomain/matches.csv"
scopus_emec_emec_path = "./data/code/matchDuplicatedNames/1_matchScopusToEMECByName/matches.csv"
scopus_emec_emec_sigla = "./data/code/matchDuplicatedNames/2_matchScopusToEMECBySigla/matches.csv"
scopus_emec_special_cases = "data/code/matchDuplicatedNames/3_matchScopusToEMECSpecialCases/matches.csv"
ror_emec_path = "./data/code/matchDuplicatedNames/5_matchRORToEMEC/matches.csv"

file_paths = [scopus_emec_domain_path, scopus_emec_emec_path, scopus_emec_emec_sigla, scopus_emec_special_cases, ror_emec_path]
output_dir = "./data/data/refined/csvs"
output_path = os.path.join(output_dir, "institutions.csv")

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Read all files from file_paths
dataframes = []
for file_path in file_paths:
    print(f"Reading {file_path}...")
    df = pd.read_csv(file_path)
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {list(df.columns)}")
    dataframes.append(df)

# Define columns to drop from each file
columns_to_drop = {
    0: ['name_match', 'url_match', 'domain_match', 'variant_match'],
    1: ['name_match', 'url_match', 'domain_match', 'variant_match'],
    2: ['name_match', 'url_match', 'domain_match', 'variant_match'],
    3: ['name_match', 'url_match', 'domain_match', 'variant_match'],
    4: ['matched_label', 'match_type', 'is_hospital', 'related_institution', 'name_match', 'url_match', 'domain_match']
}

# Drop columns from each dataframe
for i, df in enumerate(dataframes):
    cols_to_drop = [col for col in columns_to_drop[i] if col in df.columns]
    dataframes[i] = df.drop(columns=cols_to_drop)

# Rename scopus_name to name where it exists
for i, df in enumerate(dataframes):
    if 'scopus_name' in df.columns:
        dataframes[i] = df.rename(columns={'scopus_name': 'name'})

print("\nAfter dropping and renaming columns:")
for i, df in enumerate(dataframes):
    print(f"  File {i} columns: {list(df.columns)}")

# Get all unique columns across all dataframes
all_columns = set()
for df in dataframes:
    all_columns.update(df.columns)

# Add missing columns to each dataframe with NaN values
for i, df in enumerate(dataframes):
    for col in all_columns:
        if col not in df.columns:
            dataframes[i][col] = None

# Reorder columns consistently across all dataframes
dataframes = [df[sorted(list(all_columns))] for df in dataframes]

# Concatenate the dataframes
print("\nUnifying dataframes...")
unified_df = pd.concat(dataframes, ignore_index=True)
print(f"Unified shape: {unified_df.shape}")

# Save to CSV
print(f"\nSaving to {output_path}...")
unified_df.to_csv(output_path, index=False)
print("Done!")

# Print summary
print("\nSummary:")
print(f"Total rows: {len(unified_df)}")
print(f"Total columns: {len(unified_df.columns)}")
print(f"\nColumns in unified file:")
for i, col in enumerate(unified_df.columns, 1):
    print(f"  {i}. {col}")
