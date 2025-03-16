import requests
import pandas as pd
import os

def request_compliance(endpoint: str): 
    user = os.environ.get('USER', 'default_value')
    key = os.environ.get('FDA_DASHBOARD_KEY', 'default_value')

    headers = {
        "Content-Type": "application/json",
        "Authorization-User": user,  # Replace with your email
        "Authorization-Key": key  # Replace with your FDA key
    }

    # Initialize variables
    start = 1
    rows_per_request = 5000  # Maximum allowed per request
    all_data = []  # List to store all records

    while True:
        # Define request body
        data = {
            "start": start,
            "rows": rows_per_request,
            "sort": "",
            "sortorder": "",
            "filters": {},
            "columns": []  # Empty array means return all columns
        }

        # Send API request
        response = requests.post(endpoint, json=data, headers=headers)

        # Check if request is successful
        if response.status_code == 200:
            json_data = response.json()

            # Extract data from response
            batch_data = json_data.get("result", [])

            # Add batch data to the list
            all_data.extend(batch_data)

            print(f"Fetched {len(batch_data)} rows (Total so far: {len(all_data)}), moving to next batch")

            # Stop if the batch is smaller than the max request size (last batch)
            if len(batch_data) < rows_per_request:
                print("All available rows have been retrieved.")
                break

            # Move to the next batch
            start += rows_per_request
        else:
            print(f"HTTP Request Error {response.status_code}: {response.text}")
            break

    print("Saving to Local ...")

    # Convert collected data to DataFrame
    df = pd.DataFrame(all_data)

    file_name = endpoint.split("/v1/")[-1]
    print(file_name)
    # Save to Excel file
    file_path = f"./data/{file_name}.xlsx"  # Saves in the current directory
    df.to_excel(file_path, index = False)

    print(f"All data successfully saved to {file_path}")