import functions_framework
import os
import logging
import tempfile
from google.cloud import storage

logging.getLogger().setLevel(logging.INFO)

RESULT_SUFFIX = "_longest_lines.txt"

def find_longest_lines(local_path):
    """Return (max_len, list_of_(lineno, line))."""
    max_len = 0
    results = []
    with open(local_path, 'r', encoding='utf-8', errors='replace') as f:
        for lineno, raw in enumerate(f, start=1):
            line = raw.rstrip('\r\n')
            length = len(line)
            if length > max_len:
                max_len = length
                results = [(lineno, line)]
            elif length == max_len:
                results.append((lineno, line))
    return max_len, results

@functions_framework.cloud_event
def gcs_longest_lines(cloud_event):
    """Cloud Function entry point triggered by GCS events."""
    data = cloud_event.data  # Get the event payload

    # Extract bucket and file name
    bucket_name = data.get('bucket')
    object_name = data.get('name')

    # Check if the file is the result file (avoid infinite loop)
    if object_name.endswith(RESULT_SUFFIX):
        logging.info("Skipping result file: %s", object_name)
        return

    # Check required fields
    if not bucket_name or not object_name:
        logging.error("Event missing 'bucket' or 'name' fields: %s", data)
        return

    source_gs_path = f"gs://{bucket_name}/{object_name}"
    logging.info("Triggered by upload: %s", source_gs_path)

    # Initialize GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    # Temp dir
    with tempfile.TemporaryDirectory() as td:
        local_in = os.path.join(td, 'uploaded_file')

        # Download
        try:
            blob.download_to_filename(local_in)
            logging.info("Downloaded to %s", local_in)
        except Exception as e:
            logging.exception("Failed to download %s: %s", source_gs_path, e)
            return

        # Find longest lines
        try:
            max_len, longest_lines = find_longest_lines(local_in)
        except Exception as e:
            logging.exception("Failed to process file %s: %s", source_gs_path, e)
            return

        # Prepare result text
        out_lines = []
        out_lines.append(f"Source: {source_gs_path}")
        out_lines.append(f"Max length (characters, excluding newline): {max_len}")
        out_lines.append(f"Number of longest lines: {len(longest_lines)}")
        out_lines.append("")
        out_lines.append("Longest line(s):")
        for lineno, line in longest_lines:
            out_lines.append(f"[Line {lineno}] ({len(line)} chars): {line}")
        result_text = "\n".join(out_lines)

        # Log result
        logging.info("===== START =====")
        for l in result_text.split("\n"):
            logging.info(l)
        logging.info("===== END =====")

        # Upload result to GCS
        out_object_name = os.path.splitext(object_name)[0] + RESULT_SUFFIX
        result_blob = bucket.blob(out_object_name)
        try:
            result_blob.upload_from_string(result_text, content_type='text/plain')
            logging.info("Uploaded result to gs://%s/%s", bucket_name, out_object_name)
        except Exception as e:
            logging.exception("Failed to upload result to gs://%s/%s: %s", bucket_name, out_object_name, e)
            return
