import os
import csv
from google.cloud import storage

# --- Configuration ---
# The CSV file you exported from BigQuery
CSV_FILE = 'idc_results.csv'
# Local directory to save the images
DOWNLOAD_DIR = 'dicom_images'

# --- Initialize GCS client ---
storage_client = storage.Client()

# --- Create download directory ---
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- Read CSV and download ---
with open(CSV_FILE, 'r') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        gcs_url = row['gcs_url']
        # Format is 'gs://bucket-name/path/to/file.dcm'
        bucket_name = gcs_url.split('/')[2]
        blob_path = '/'.join(gcs_url.split('/')[3:])

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # Create a safe local filename
        local_filename = f"patient_{row['PatientID']}_series_{row['SeriesInstanceUID']}.dcm"
        local_path = os.path.join(DOWNLOAD_DIR, local_filename)

        print(f"Downloading {gcs_url} to {local_path}...")
        blob.download_to_filename(local_path)

        # Optional: stop after a few files for testing
        # if i > 5: break
