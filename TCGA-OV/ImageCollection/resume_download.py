import os
import csv
from google.cloud import storage

CSV_FILE = 'idc_results.csv'
DOWNLOAD_DIR = 'dicom_images'

# Initialize clients
storage_client = storage.Client()
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

downloaded_count = 0
skipped_count = 0

with open(CSV_FILE, 'r') as f:
    reader = csv.DictReader(f)
    total_rows = sum(1 for _ in open(CSV_FILE)) - 1  # subtract header
    f.seek(0)  # reset to beginning
    next(reader)  # skip header row for actual reading
    
    for i, row in enumerate(reader):
        gcs_url = row['gcs_url']
        sop_uid = row['SOPInstanceUID']
        patient_id = row['PatientID']
        
        # Unique filename using SOPInstanceUID
        local_filename = f"patient_{patient_id}_sop_{sop_uid}.dcm"
        local_path = os.path.join(DOWNLOAD_DIR, local_filename)
        
        # Skip if already exists
        if os.path.exists(local_path):
            skipped_count += 1
            if skipped_count % 100 == 0:
                print(f"Skipped {skipped_count} existing files...")
            continue
        
        # Download
        try:
            bucket_name = gcs_url.split('/')[2]
            blob_path = '/'.join(gcs_url.split('/')[3:])
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            print(f"[{i+1}/{total_rows}] Downloading: {local_filename}")
            blob.download_to_filename(local_path)
            downloaded_count += 1
            
        except Exception as e:
            print(f"Error downloading {gcs_url}: {e}")
            # Optionally log failed UIDs to a file for retry later

print(f"\nResume complete. Downloaded: {downloaded_count}, Skipped: {skipped_count}")
