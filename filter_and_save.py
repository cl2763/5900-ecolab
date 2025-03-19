import pandas as pd
from pathlib import Path

def filter_and_save_datasets():
    # Get the data directory paths
    data_dir = Path('data')
    original_dir = data_dir / 'original_csv'
    filtered_dir = data_dir / 'filtered'
    
    # Dictionary of filters for each dataset
    filters = {
        'inspections_citations.csv': {
            'filter_col': 'ProgramArea',
            'filter_value': 'Foods'
        },
        'compliance_actions.csv': {
            'filter_col': 'ProductType',
            'filter_value': 'Food/Cosmetics'
        },
        'inspections_classifications.csv': {
            'filter_col': 'ProductType',
            'filter_value': 'Food/Cosmetics'
        },
        'recall_data.csv': {
            'filter_col': 'PRODUCTTYPESHORT',
            'filter_value': 'Food'
        }
    }
    
    for file_name, filter_info in filters.items():
        try:
            print(f"\nProcessing {file_name}...")
            
            # Read the CSV file from original directory
            input_path = original_dir / file_name
            df = pd.read_csv(input_path)
            
            # Apply filter
            if isinstance(filter_info['filter_value'], list):
                # For multiple values (Food/Cosmetics)
                filtered_df = df[df[filter_info['filter_col']].isin(filter_info['filter_value'])]
            else:
                # For single value (Food or Food/Cosmetics)
                filtered_df = df[df[filter_info['filter_col']] == filter_info['filter_value']]
            
            # Create output filename
            output_filename = file_name.replace('.csv', '_filtered.csv')
            output_path = filtered_dir / output_filename
            
            # Save filtered data as regular CSV
            filtered_df.to_csv(output_path, index=False)
            
            print(f"Original rows: {len(df)}")
            print(f"Filtered rows: {len(filtered_df)}")
            print(f"Saved filtered data to: {output_filename}")
            
        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")

if __name__ == "__main__":
    filter_and_save_datasets() 