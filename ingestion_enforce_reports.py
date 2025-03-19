#import required libraries
import requests
import datetime
import pandas as pd
import os
import re

def request_recall(): 
    def clean_text(value):
        #Remove non-printable characters from text fields
        if isinstance(value, str):
            return re.sub(r'[\x00-\x1F\x7F-\x9F]', '', value)  # Remove control characters
        return value

    user = os.environ.get('USER', 'default_value')
    key = os.environ.get('ENFORCE_REPORT_KEY', 'default_value')

    # Create a signature and append it to the URL to avoid cached responses from server.
    signature = str(int(datetime.datetime.now().timestamp()))

    # Set the API URL
    url = 'https://www.accessdata.fda.gov/rest/iresapi/recalls/?signature=' + signature

    # Set headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization-User': user,
        'Authorization-Key': key
    }

    # Columns to fetch
    display_columns = "firmfeinum,firmlegalnam,firmsurvivingnam,producttypeshort,centerclassificationtypetxt,phasetxt,distributionareasummarytxt,firmcitynam,firmcountrynam,centerclassificationdt,productshortreasontxt,productdescriptiontxt,recalleventid,productid,centercd"

    # Pagination variables
    start = 1
    rows_per_request = 5000
    all_data = []  # List to store all fetched data

    while True:
        # Build the payload dynamically with pagination
        data = f'payload={{"displaycolumns": "{display_columns}", \
        "filter" : "[]", \
        "start": {start}, \
        "rows" : {rows_per_request}, \
        "sort" : "productid", \
        "sortorder": "asc"}}'

        # Send the request
        response = requests.post(url, headers=headers, data=data)

        # Check if request was successful
        if response.status_code == 200:
            json_response = response.json()

            # Extract "RESULT" key, which contains the recall data
            if "RESULT" in json_response:
                recall_data = json_response["RESULT"]

                # If no more data is returned, stop pagination
                if not recall_data:
                    print("No more data to fetch.")
                    break

                # Append fetched data to list
                all_data.extend(recall_data)

                # Move to the next batch
                start += rows_per_request
                print(f"Fetched {len(recall_data)} rows (Total so far: {len(all_data)}), moving to next batch...")
            else:
                print("Error: 'RESULT' key not found in response.")
                break
        else:
            print(f"Error: {response.status_code}, {response.text}")
            break

    # Convert the collected data into a Pandas DataFrame
    df = pd.DataFrame(all_data)

    # Apply cleaning function only to object (string) columns
    df = df.apply(lambda col: col.map(clean_text) if col.dtype == "object" else col)

    # Save DataFrame to Exceli
    file_path = "./data/recall_data.xlsx"
    df.to_excel(file_path, index=False)

    print(f"Data successfully saved to {file_path}, total records: {len(df)}")
