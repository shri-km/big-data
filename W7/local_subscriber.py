import json
import os
import logging
from google.cloud import pubsub_v1, storage
from time import sleep
from google.api_core.exceptions import GoogleAPICallError

logging.basicConfig(level=logging.INFO)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
SUBSCRIPTION_NAME = os.environ.get("PUBSUB_SUB_LOCAL", "sub-local1")
GCS_OUTPUT_PREFIX = os.environ.get("W7_OUTPUT_PREFIX", "w7/outputs/")

storage_client = storage.Client()
subscriber_client = pubsub_v1.SubscriberClient()
sub_path = subscriber_client.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

def process_message(message_dict):
    bucket = message_dict["bucket"]
    name = message_dict["name"]
    bucket_obj = storage_client.bucket(bucket)
    blob = bucket_obj.blob(name)
    content = blob.download_as_text().splitlines()

    max_len = 0
    longest_lines = []
    for line in content:
        l = len(line)
        if l > max_len:
            max_len = l
            longest_lines = [line]
        elif l == max_len:
            longest_lines.append(line)

    base_name = os.path.basename(name)
    output_filename = f"{os.path.splitext(base_name)[0]}_local_longest.txt"
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

    print("=== Subscriber (Local) Result ===")
    print(output_content)
    print(f"Saved output to gs://{bucket}/{output_path}")

def callback(msg):
    try:
        data = msg.data.decode("utf-8")
        message_dict = json.loads(data)
        print(f"Received message: {message_dict}")
        process_message(message_dict)
        msg.ack()
        print("Message acknowledged.")
    except Exception as e:
        print(f"Error processing message: {e}")
        msg.nack()

def main():
    # Start the streaming pull
    streaming_pull_future = subscriber_client.subscribe(sub_path, callback=callback)
    logging.info(f"Listening for messages on {sub_path}...")

    try:
        streaming_pull_future.result() 

    except GoogleAPICallError as e:
        logging.error(f"Stream error: {e}")
        
    except KeyboardInterrupt:
        # Shutdown when interrupted by Ctrl+C
        print("\nReceived interrupt. Shutting down subscriber...")
        streaming_pull_future.cancel()
        
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main loop: {e}")

    finally:
        if not streaming_pull_future.done():
             streaming_pull_future.cancel()
        subscriber_client.close()
        logging.info("Listener stopped and client closed.")

if __name__ == "__main__":
    main()