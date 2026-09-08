import os
import functions_framework
import json
from google.cloud import pubsub_v1
# Add google-cloud-pubsub to requirements

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
TOPIC_NAME = os.environ.get("PUBSUB_TOPIC", "file-events-topic")
INPUT_FOLDER = os.environ.get("W7_INPUT_FOLDER", "w7")
OUTPUT_FOLDER = os.environ.get("W7_OUTPUT_FOLDER", "w7/outputs")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)


@functions_framework.cloud_event
def gcs_publisher(event):
    """
    Triggered by a file upload to a GCS bucket.
    Publishes file metadata to a Pub/Sub topic.
    """

    event_data = event.data

    bucket = event_data.get("bucket")
    name = event_data.get("name")
    time_created = event_data.get("timeCreated")
    size = event_data.get("size")

    if name.startswith(f"{INPUT_FOLDER}/") and not name.startswith(f"{OUTPUT_FOLDER}/"):
        message_dict = {
            "bucket": bucket,
            "name": name,
            "timeCreated": time_created,
            "size": size,
        }

        data_str = json.dumps(message_dict)

        # Publish as bytes to Pub/Sub
        future = publisher.publish(topic_path, data_str.encode("utf-8"))
        message_id = future.result()

        print(f"Published message id {message_id} for file gs://{bucket}/{name}")

    else:
        print(f"File {name} is not in the concerned folder, ignoring.")