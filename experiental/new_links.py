import pandas as pd
import os

def find_new_urls(new_file, old_file, output_file):
    print(f"Comparing URLs in {new_file} and {old_file}...")
    
    # Read the CSV files
    try:
        new_df = pd.read_csv(new_file)
        old_df = pd.read_csv(old_file)
    except Exception as e:
        print(f"Error reading CSV files: {e}")
        return
    
    # Check if 'url' column exists in both dataframes
    if 'url' not in new_df.columns or 'url' not in old_df.columns:
        print("Error: One or both CSV files missing 'url' column")
        print(f"New file columns: {new_df.columns.tolist()}")
        print(f"Old file columns: {old_df.columns.tolist()}")
        return
    
    # Extract URLs
    new_urls = set(new_df['url'].tolist())
    old_urls = set(old_df['url'].tolist())
    
    # Find URLs in new file but not in old file
    unique_new_urls = new_urls - old_urls
    
    # Statistics
    print(f"Total URLs in new file: {len(new_urls)}")
    print(f"Total URLs in old file: {len(old_urls)}")
    print(f"URLs unique to new file: {len(unique_new_urls)}")
    
    # Create DataFrame with new URLs
    result_df = new_df[new_df['url'].isin(unique_new_urls)]
    
    # Save to CSV
    result_df.to_csv(output_file, index=False)
    print(f"New URLs saved to {output_file}")

if __name__ == "__main__":
    # File paths
    new_file = 'yahoo_links_20250604.csv'
    old_file = 'yahoo_links_2024.csv'
    output_file = 'yahoo_new_urls_2025.csv'
    
    # Ensure files exist
    if not os.path.exists(new_file):
        print(f"Error: New file {new_file} not found")
    elif not os.path.exists(old_file):
        print(f"Error: Old file {old_file} not found")
    else:
        find_new_urls(new_file, old_file, output_file)