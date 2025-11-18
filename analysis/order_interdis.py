import pandas as pd
from pathlib import Path

macro_folder = Path('./analysis/data/macro')
sub_folder = Path('./analysis/data/sub')

macro_files = list(macro_folder.glob('**/interdis.csv'))
sub_files = list(sub_folder.glob('**/interdis.csv'))
interdis_files = macro_files + sub_files

print(f"Found {len(macro_files)} interdis.csv files in macro folder")
print(f"Found {len(sub_files)} interdis.csv files in sub folder")
print(f"Total: {len(interdis_files)} files\n")

for file_path in interdis_files:
    print(f"\nProcessing: {file_path}")
    
    df = pd.read_csv(file_path, encoding='utf-8')
    
    df_sorted = df.sort_values('DIV_STAR', ascending=False)
    df_sorted.index = pd.RangeIndex(start=1, stop=len(df) + 1)

    output_path = file_path.parent / 'sorted_interdis.csv'
    df_sorted.to_csv(output_path, encoding='utf-8')
    
    print(f"✓ Sorted {len(df_sorted)} rows by DIV_STAR (descending)")
    print(f"  Saved to: {output_path}")

print("\n✓ All files ordered successfully!")
