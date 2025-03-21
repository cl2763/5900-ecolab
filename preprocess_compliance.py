import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import random

# List of common user agents to rotate through
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
]

def format_date(date_str):
    """Convert date string to MMDDYYYY format"""
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj.strftime('%m%d%Y')
    except:
        return None

def generate_warning_letter_link(row):
    """Generate warning letter link based on the specified format"""
    if row['ActionType'] != 'Warning Letter':
        return None
    
    # Convert firm name to lowercase and remove special characters
    firm_name = str(row['LegalName']).lower()
    firm_name = ''.join(c for c in firm_name if c.isalnum() or c.isspace() or c == '&')
    firm_name = firm_name.replace(' ', '-').replace('&', '')
    
    # Get CaseInjunctionID and format date
    case_id = str(row['CaseInjunctionID'])
    action_date = format_date(row['ActionTakenDate'])
    
    if not action_date:
        return None
    
    # Generate the link
    link = f"https://www.fda.gov/inspections-compliance-enforcement-and-criminal-investigations/warning-letters/{firm_name}-{case_id}-{action_date}"
    return link

def check_link_validity(url):
    """Check if the URL is valid and accessible"""
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    try:
        # First try a HEAD request
        response = requests.head(url, 
                               headers=headers, 
                               allow_redirects=True, 
                               timeout=5)
        
        # If we get a 404, the page definitely doesn't exist
        if response.status_code == 404:
            return False
            
        # For 403 (Forbidden) responses, try a GET request to verify
        # Some servers might block HEAD requests but allow GET
        if response.status_code == 403:
            try:
                get_response = requests.get(url, 
                                         headers=headers, 
                                         allow_redirects=True, 
                                         timeout=5)
                
                # Check if the page content indicates it's not found
                if any(phrase in get_response.text.lower() for phrase in [
                    'page not found',
                    'cannot be found',
                    'no longer exists',
                    'not available',
                    'error 404'
                ]):
                    return False
                    
                # If we got here, the page exists but is blocked
                return True
                
            except:
                # If GET request fails, assume the page exists but is blocked
                return True
        
        # For all other status codes, consider the link invalid
        # This is more conservative - only accept 200 (OK) responses
        return response.status_code == 200
            
    except Exception as e:
        print(f"\nError checking URL: {url}")
        print(f"Error: {str(e)}")
        # If we can't validate the link, consider it invalid
        return False

def validate_link_batch(links_batch):
    """Validate a batch of links and return results"""
    results = []
    for idx, link in links_batch:
        is_valid = check_link_validity(link)
        if not is_valid:
            print(f"\nInvalid link found: {link}")
        results.append((idx, is_valid))
        # Random delay between 1 and 3 seconds to avoid overwhelming the server
        time.sleep(random.uniform(1, 3))
    return results

def preprocess_compliance_data():
    # Get the data directory paths
    data_dir = Path('data')
    input_file = data_dir / 'filtered' / 'compliance_actions_filtered.csv'
    output_file = data_dir / 'processed_data' / 'compliance_actions_processed.csv'
    
    print("Reading compliance actions data...")
    df = pd.read_csv(input_file)
    
    # 1. Handle Missing Values
    print("Handling missing values...")
    df = df.fillna('unavailable')
    
    # 2. Drop Unnecessary Columns
    print("Dropping unnecessary columns...")
    columns_to_drop = ['ProductType', 'Center']
    df = df.drop(columns=columns_to_drop, errors='ignore')
    
    # 3. Generate Warning Letter Links
    print("Generating warning letter links...")
    df['WarningLetterLink'] = df.apply(generate_warning_letter_link, axis=1)
    
    # Print example links for debugging
    print("\nExample links generated:")
    for idx, row in df[df['WarningLetterLink'].notna()].head(3).iterrows():
        print(f"\nRow {idx}:")
        print(f"LegalName: {row['LegalName']}")
        print(f"CaseInjunctionID: {row['CaseInjunctionID']}")
        print(f"ActionTakenDate: {row['ActionTakenDate']}")
        print(f"Generated Link: {row['WarningLetterLink']}")
        print(f"Link Valid: {check_link_validity(row['WarningLetterLink'])}")
    
    # Print specific example for JR & Son World Trading LLC
    print("\nSpecific example for JR & Son World Trading LLC:")
    jr_son_row = df[df['LegalName'] == 'JR & Son World Trading LLC'].iloc[0]
    print(f"LegalName: {jr_son_row['LegalName']}")
    print(f"CaseInjunctionID: {jr_son_row['CaseInjunctionID']}")
    print(f"ActionTakenDate: {jr_son_row['ActionTakenDate']}")
    print(f"Generated Link: {jr_son_row['WarningLetterLink']}")
    print(f"Link Valid: {check_link_validity(jr_son_row['WarningLetterLink'])}")
    
    # 4. Validate Links with parallel processing
    print("\nValidating warning letter links...")
    warning_letter_rows = df[df['WarningLetterLink'].notna()]
    total_links = len(warning_letter_rows)
    print(f"Found {total_links} warning letter links to validate")
    
    # Prepare batches of links for parallel processing
    batch_size = 10
    link_batches = []
    current_batch = []
    
    for idx, row in warning_letter_rows.iterrows():
        current_batch.append((idx, row['WarningLetterLink']))
        if len(current_batch) == batch_size:
            link_batches.append(current_batch)
            current_batch = []
    
    if current_batch:
        link_batches.append(current_batch)
    
    # Process batches in parallel using ThreadPoolExecutor for concurrent execution
    # Using 5 worker threads to balance between performance and system resources
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit each batch to the thread pool and collect futures
        # Each future represents the eventual result of validating one batch of links
        futures = [executor.submit(validate_link_batch, batch) for batch in link_batches]
        
        # Create progress bar to track validation progress
        with tqdm(total=total_links, desc="Validating links") as pbar:
            # Process futures as they complete (in any order)
            for future in as_completed(futures):
                # Get validation results for this batch
                results = future.result()
                
                # Process each individual link validation result
                for idx, is_valid in results:
                    # If link is invalid, update the DataFrame with a message pointing to firm profile
                    if not is_valid:
                        df.at[idx, 'WarningLetterLink'] = f"no direct link access, find warning letters in firm profile instead: {df.at[idx, 'FirmProfile']}"
                    # Update progress bar
                    pbar.update(1)
    
    # Save processed data
    print("\nSaving processed data...")
    df.to_csv(output_file, index=False)
    print(f"Processed data saved to: {output_file}")
    
    # Print summary
    print("\nProcessing Summary:")
    print(f"Total rows processed: {len(df)}")
    print(f"Warning letter links generated: {len(df[df['WarningLetterLink'].notna()])}")
    print(f"Valid warning letter links: {len(df[df['WarningLetterLink'].str.startswith('https://', na=False)])}")

if __name__ == "__main__":
    preprocess_compliance_data()