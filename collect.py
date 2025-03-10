import ingestion_dashboard
import ingestion_enforce_reports
import ingestion_483s
import os

def delete_all_files(directory):
    # Check if the directory exists
    if os.path.exists(directory) and os.path.isdir(directory):
        # Loop through all files in the directory
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            # Check if it's a file (not a subdirectory)
            if os.path.isfile(file_path):
                os.remove(file_path)  # Delete the file
                print(f"Deleted: {file_path}")
    else:
        print("The specified directory does not exist.")

delete_all_files("./data")

inspec_class_url = "https://api-datadashboard.fda.gov/v1/inspections_classifications" 
inspec_citation_url = "https://api-datadashboard.fda.gov/v1/inspections_citations"

ingestion_dashboard.request_inspection(inspec_class_url)

ingestion_dashboard.request_inspection(inspec_citation_url)

ingestion_enforce_reports.request_recall()

ingestion_483s.download_483s()