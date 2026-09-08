# Big Data - OPPE 1

## Overview

This assignment uses Google Cloud Dataproc and PySpark to analyze Indian railway stations data.
The job reads input data from GCS, performs analysis and writes results back to GCS.

## Datasets

1. `Train_details_22122017.csv` - Customer transaction details

## Files in this Submission

### 1. `oppe.py`

The main script that performs all the analysis.

**What it does:**  

- Loads the input dataset — transactions from GCS.
- Runs the aggregation queries:
- Writes the final aggregated stats output back to GCS as CSV.
- Logs and displays preview results in the driver output for validation.

### 2. `gcf.py`

Cloud Function script

- Gets triggered in case of file upload to GCS.
- Runs the function to find the maximum 99th percentile of the durations trains stayed in the station.

### `csv`

Output written by the main script.
Output file was written to gs://ob-bigdata/oppe/station_stats/

## Pipeline

1. **Upload**: Upload dataset and the python script to the GCS bucket.

2. **Run the Spark Job**: Run the script (`oppe1.py`) on a Google Cloud Dataproc cluster. The job automatically reads from GCS, processes data, and writes back results.

3. **Output**: All results are stored in GCS CSV output.

4. **Cloud Function**: Cloud Function gets triggered and finds the max 99th percentile time.

---
