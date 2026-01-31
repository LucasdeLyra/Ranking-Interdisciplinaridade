import pandas as pd
from pathlib import Path

# Define the base path for data
base_path = Path(__file__).parent / "data"

# Define all folder paths to process
folder_paths = [
    # Macro folders
    base_path / "macro" / "macro_no_quality",
    base_path / "macro" / "macro_percentile",
    base_path / "macro" / "macro_q1",
    base_path / "macro" / "macro_q2",
    base_path / "macro" / "macro_q3",
    base_path / "macro" / "macro_q4",
    base_path / "macro" / "macro_quartile",
    # Sub folders
    base_path / "sub" / "sub_no_quality",
    base_path / "sub" / "sub_percentile",
    base_path / "sub" / "sub_q1",
    base_path / "sub" / "sub_q2",
    base_path / "sub" / "sub_q3",
    base_path / "sub" / "sub_q4",
    base_path / "sub" / "sub_quartile",
]

def process_folder(folder_path):
    """
    Process a folder containing rank.txt, labels.csv, and institutions_names.csv files.
    Returns a dataframe with 'area' and 'average' columns.
    
    Structure:
    - Each row in rank.txt corresponds to an area (in order of labels.csv)
    - Each column in rank.txt corresponds to an institution
    - We calculate the average across all institutions for each area
    """
    rank_file = folder_path / "rank.txt"
    labels_file = folder_path / "labels.csv"
    
    if not rank_file.exists() or not labels_file.exists():
        print(f"Warning: Missing files in {folder_path}")
        return None
    
    # Read the files
    # rank.txt contains numeric data (one row per area, one column per institution)
    rank_data = pd.read_csv(rank_file, header=None)
    
    # labels.csv contains the area names (should match number of rows in rank.txt)
    labels_df = pd.read_csv(labels_file, header=None)
    labels = labels_df.iloc[:, 0].tolist()
    
    # Calculate row averages (average across all institutions for each area)
    row_averages = rank_data.mean(axis=1).values
    
    # Create result dataframe
    result_df = pd.DataFrame({
        'area': labels,
        'average': row_averages
    })
    
    return result_df, folder_path.name

# Process all folders
for folder_path in folder_paths:
    if not folder_path.exists():
        print(f"Folder does not exist: {folder_path}")
        continue
    
    result = process_folder(folder_path)
    if result is not None:
        result_df, folder_name = result
        
        # Save to CSV in the same folder
        output_file = folder_path / "averages.csv"
        result_df.to_csv(output_file, index=False)
        print(f"✓ Processed {folder_name}: {len(result_df)} areas")
        print(f"  Saved to: {output_file}")
        print(f"  Sample:\n{result_df.head()}\n")
    else:
        print(f"✗ Failed to process {folder_path.name}\n")

print("Done! All averages have been calculated and saved.")
