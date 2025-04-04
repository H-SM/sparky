import pandas as pd
import numpy as np

def convert_txt_to_csv(input_file, output_file):
    """
    Convert text edge list to CSV file
    
    Parameters:
    input_file (str): Path to input text file
    output_file (str): Path to output CSV file
    """
    # Read the text file
    try:
        # Read space-separated values
        df = pd.read_csv(input_file, sep=' ', header=None, names=['source', 'destination'])
        
        # Remove duplicate edges
        df = df.drop_duplicates()
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        
        print(f"Conversion complete. CSV saved to {output_file}")
        print(f"Total edges: {len(df)}")
        print(f"Unique source nodes: {df['source'].nunique()}")
        print(f"Unique destination nodes: {df['destination'].nunique()}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    # Input and output file paths
    input_file = 'twitter_combined.txt'  # Replace with your input file path
    output_file = 'twitter_social_network.csv'
    
    # Convert txt to CSV
    convert_txt_to_csv(input_file, output_file)

if __name__ == "__main__":
    main()