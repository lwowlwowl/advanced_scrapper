import pandas as pd
import numpy as np

# Input parameters
input_file = 'yahoo_new_urls_2025.csv'
output_prefix = 'parts/yfin_2025'
drop_file = 'success_articles_2025_yfin.csv'
num_parts = 2

def split_csv(input_file, output_prefix, num_parts):
    # Read the CSV file
    df = pd.read_csv(input_file)
    drop_df = pd.read_csv(drop_file)
    print(df)
    df = df[~df['url'].isin(drop_df['url'])]
    print(df)

    
    # Create an array to cycle through the part numbers
    part_numbers = np.arange(num_parts)
    
    # Assign part numbers to each row
    df['part'] = np.tile(part_numbers, len(df) // num_parts + 1)[:len(df)]
    
    # Split and save
    for i in range(num_parts):
        part_df = df[df['part'] == i].drop('part', axis=1)
        print(i)
        print(part_df)
        part_df.to_csv(f"{output_prefix}_{i}.csv", index=False)

    print(f"CSV file has been split into {num_parts} parts.")

# Run the function
split_csv(input_file, output_prefix, num_parts)