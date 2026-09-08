# Write a Google Cloud Function that:

# Triggers when the CSV result file is uploaded to the GCS bucket.
# Reads the result file and prints the station having the highest 99th percentile stop duration.

import functions_framework
import os
import tempfile
import pandas as pd
from google.cloud import storage

@functions_framework.cloud_event
def gcs_csv_handler(event):
    """
    Trigger: Cloud Storage 'finalize' event when a file is uploaded.
    This function expects the uploaded object is the CSV of station stats,
    with a column named 'p99_duration' and 'Station Name'.
    """

    data = event.data
    
    bucket_name = data.get('bucket')
    object_name = data.get('name')

    print(f"Triggered by file: gs://{bucket_name}/{object_name}")

    #Only process csv files
    if not object_name.endswith(".csv"):
        print("File is not csv - ignoring.")
        return

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    # download to temp file
    _, temp_local = tempfile.mkstemp(suffix=".csv")
    blob.download_to_filename(temp_local)

    try:
        df = pd.read_csv(temp_local)
    except Exception as e:
        print("Error reading CSV:", e)
        return

    p99_col = 'p99_duration'

    df = df.dropna(subset=[p99_col])
    df[p99_col] = pd.to_numeric(df[p99_col], errors='coerce')
    df = df.dropna(subset=[p99_col])

    if df.empty:
        print("No valid p99 values found.")
        return

    max_row = df.loc[df[p99_col].idxmax()]
    station_name = max_row.get('Station Name') or max_row.get('Station_Name') or max_row.get('station')
    max_p99 = max_row[p99_col]

    print(f"Station with highest 99th percentile stop duration: {station_name} (p99 = {max_p99})")
