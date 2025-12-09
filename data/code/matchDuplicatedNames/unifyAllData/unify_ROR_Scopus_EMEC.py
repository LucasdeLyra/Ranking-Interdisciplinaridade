import pandas as pd
import os

# Define file paths
scopus_emec_path = "./data/code/matchDuplicatedNames/matchScopusToEMEC/matches.csv"
ror_emec_path = "./data/code/matchDuplicatedNames/matchRORToEMEC/matches.csv"
output_dir = "./data/data/refined/csvs"
output_path = os.path.join(output_dir, "institutions.csv")

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Read both files
print("Reading matchScopusToEMEC/matches.csv...")
scopus_df = pd.read_csv(scopus_emec_path)
print(f"  Shape: {scopus_df.shape}")
print(f"  Columns: {list(scopus_df.columns)}")

print("\nReading matchRORToEMEC/matches.csv...")
ror_df = pd.read_csv(ror_emec_path)
print(f"  Shape: {ror_df.shape}")
print(f"  Columns: {list(ror_df.columns)}")

# Get common columns (first 31 columns are the same in both files - from EMEC data)
common_cols = [col for col in scopus_df.columns if col in ror_df.columns]
print(f"\nCommon columns: {len(common_cols)}")

# Get unique columns from each file
scopus_unique = [col for col in scopus_df.columns if col not in ror_df.columns]
ror_unique = [col for col in ror_df.columns if col not in scopus_df.columns]

print(f"Unique to Scopus file: {scopus_unique}")
print(f"Unique to ROR file: {ror_unique}")

# Columns to drop
scopus_drop_cols = ['name_match', 'url_match', 'domain_match', 'variant_match']
ror_drop_cols = ['matched_label', 'match_type', 'is_hospital', 'related_institution', 'name_match', 'url_match', 'domain_match']

# Drop columns from each dataframe
scopus_df = scopus_df.drop(columns=[col for col in scopus_drop_cols if col in scopus_df.columns])
ror_df = ror_df.drop(columns=[col for col in ror_drop_cols if col in ror_df.columns])

# Rename scopus_name to name in ROR dataframe
if 'scopus_name' in ror_df.columns:
    ror_df = ror_df.rename(columns={'scopus_name': 'name'})

print("\nAfter dropping and renaming columns:")
print(f"  Scopus columns: {list(scopus_df.columns)}")
print(f"  ROR columns: {list(ror_df.columns)}")

# Get common columns again after dropping
common_cols = [col for col in scopus_df.columns if col in ror_df.columns]
print(f"\nCommon columns after cleanup: {len(common_cols)}")

# Get unique columns from each file
scopus_unique = [col for col in scopus_df.columns if col not in ror_df.columns]
ror_unique = [col for col in ror_df.columns if col not in scopus_df.columns]

print(f"Unique to Scopus file: {scopus_unique}")
print(f"Unique to ROR file: {ror_unique}")

# Add missing columns to each dataframe with NaN values
for col in scopus_unique:
    ror_df[col] = None

for col in ror_unique:
    scopus_df[col] = None

# Reorder columns to have common columns first, then Scopus unique, then ROR unique
all_cols = common_cols + scopus_unique + ror_unique
scopus_df = scopus_df[all_cols]
ror_df = ror_df[all_cols]

# Concatenate the dataframes
print("\nUnifying dataframes...")
unified_df = pd.concat([scopus_df, ror_df], ignore_index=True)
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
