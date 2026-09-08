import functions_framework
import base64
import json
import os
from google.cloud import storage
# Add google-cloud-storage to requirements

GCS_OUTPUT_PREFIX = os.environ.get("W7_OUTPUT_PREFIX", "w7/outputs/")

storage_client = storage.Client()

def process_message_data(message_dict):
    bucket = message_dict["bucket"]
    name = message_dict["name"]

    bucket_obj = storage_client.bucket(bucket)
    blob = bucket_obj.blob(name)
    
    content_bytes = blob.download_as_bytes()
    content = content_bytes.decode("utf-8", errors="replace").splitlines()

    # Find max length
    max_len = 0
    longest_lines = []
    for line in content:
        l = len(line)
        if l > max_len:
            max_len = l
            longest_lines = [line]
        elif l == max_len:
            longest_lines.append(line)

    # prepare output
    base_name = os.path.basename(name)
    output_filename = f"{os.path.splitext(base_name)[0]}_cloudfunc_longest.txt"
    output_path = f"{GCS_OUTPUT_PREFIX}{output_filename}"
    output_blob = bucket_obj.blob(output_path)
    output_text = [
        f"Input file: {name.split('/')[-1]}",
        f"Maximum line length: {max_len}",
        "Longest line(s):",
        *longest_lines,
    ]
    output_content = "\n".join(output_text)
    output_blob.upload_from_string(output_content, content_type='text/plain')

    print("=== Subscriber (Cloud Function) Result ===")
    print(output_content)
    print(f"Saved output to gs://{bucket}/{output_path}")

@functions_framework.cloud_event
def pubsub_subscriber_cf(event):
    """
    Triggered from a message on a Cloud Pub/Sub topic.
    """

    inner_event_dict = event.data 

    print(f"Inner event dictionary type: {type(inner_event_dict)}")
    
    if not isinstance(inner_event_dict, dict):
        print("ERROR: event.data is not a dictionary. Cannot proceed.")
        return

    base64_data = None
    
    try:
        base64_data = inner_event_dict['message']['data']
        print("SUCCESS: Found base64 string at inner_event_dict['message']['data'].")
    except KeyError as e:
        print(f"ERROR (Key): Failed at deep nesting check. Key {e} was missing in the inner dictionary.")
        return
    except TypeError as e:
        print(f"ERROR (Type): Failed at deep nesting check. An intermediate value was not a dictionary. Error: {e}")
        return

    if not base64_data:
        print("FATAL: Could not retrieve base64 data.")
        return

    try:
        print(f"Base64 string retrieved. Starting decode...")
        data_bytes = base64.b64decode(base64_data)
        data_str = data_bytes.decode('utf-8')
        message_dict = json.loads(data_str)
        process_message_data(message_dict) 
        
        print(f"--- SUCCESS: Message processed. Final dict: {message_dict} ---")

    except Exception as e:
        print(f"UNEXPECTED PROCESSING ERROR: {e}")
