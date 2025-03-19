import pandas as pd
import os
from pathlib import Path

def convert_excel_to_csv():
    # Get the data directory paths
    data_dir = Path('data')
    original_dir = data_dir / 'original'
    
    # Create original directory if it doesn't exist
    original_dir.mkdir(exist_ok=True)
    
    # Get all Excel files in the data directory
    excel_files = list(data_dir.glob('*.xlsx'))
    
    for excel_file in excel_files:
        try:
            # Read the Excel file
            print(f"Converting {excel_file.name} to CSV...")
            df = pd.read_excel(excel_file)
            
            # Create the CSV filename
            csv_filename = excel_file.stem + '.csv'
            csv_path = original_dir / csv_filename
            
            # Save as regular CSV
            df.to_csv(csv_path, index=False)
            print(f"Successfully converted {excel_file.name} to {csv_filename}")
            
        except Exception as e:
            print(f"Error converting {excel_file.name}: {str(e)}")

if __name__ == "__main__":
    convert_excel_to_csv()