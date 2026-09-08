# Big Data - Week 7 Assignment

## Overview

This assignment demonstrates an event-driven workflow on Google Cloud Platform (GCP) that automatically reacts when a text file is uploaded to a Cloud Storage (GCS) bucket.

The upload triggers a publisher Cloud Function, which publishes the file metadata to a Pub/Sub topic. Three independent subscribers—a Cloud Function, a Compute Engine VM script, and a local machine script—listen to the same topic.

Each subscriber fetches the file from GCS, determines the longest line(s) in the text file, and writes an output file to GCS containing:

- The input file name
- The maximum line length
- The longest line contents

All subscribers operate independently yet perform identical logic, demonstrating multi-subscriber Pub/Sub architecture.

## Files in this Submission

### Input File - `sample.txt`

The file that is uploaded to the GCS bucket. It contains two lines of identical maximum length to test tie-handling.

### `publisher_function.py`

Cloud Function triggered by GCS file upload. It publishes the file metadata (bucket name and file name) to a Pub/Sub topic.

### `subscriber_function.py`

Cloud Function subscribed to the Pub/Sub topic.
On receiving a message, it downloads the file from GCS, finds the longest line(s), and writes the output back to GCS.

### Python scripts - `local_subscriber.py` and `vm_subscriber.py`

Local Machine and Cloud Compute VM subscriber scripts that connects to Pub/Sub, receives messages, downloads the file, and writes the output to GCS.

### Output Files

3 output files (inside the outputs folder) produced by Cloud Function, Local Machine, and Compute Engine VM.

### `vm_init.sh`

Shell script to install necessary packages during VM setup.

## Pipeline Overview

Check the screencast for a more detailed explanation.

- Create Pub/Sub Topic and 2 subscriptions for Local and Cloud Compute Machines.
- Deploy Cloud Functions:
    -- Publisher: Trigerred by GCS Upload
    -- Subscriber: Triggerred by Pub/Sub message
- Setup Compute Engine VM, SSH. Upload python and shell script. Run vm_init.sh. Activate venv and run VM subscriber script.
- Authenticate Local Machine and run the local subscriber script.
- Upload the input file to GCS. It will be processed by the 3 subscribers and the outputs will be written back to GCS.

---
