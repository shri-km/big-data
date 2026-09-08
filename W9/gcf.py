import os
import uuid
import json
import functions_framework
from googleapiclient import discovery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
REGION = os.environ.get("GCP_REGION", "asia-south1")
CLUSTER_NAME = os.environ.get("DATAPROC_CLUSTER_NAME", "cluster-bigdata")

_bucket = os.environ.get("GCS_BUCKET", "your-gcs-bucket-name").replace("gs://", "").strip("/")
INFERENCE_PY = os.environ.get("W9_INFERENCE_PY", f"gs://{_bucket}/w9/predict.py")
PREDICTIONS_OUT = os.environ.get("W9_PREDICTIONS_OUT", f"gs://{_bucket}/w9/predictions/")

dataproc = discovery.build("dataproc", "v1", cache_discovery=False)

def submit_job(test_path, output_path):
    job_id = f"infer-{uuid.uuid4().hex[:8]}"

    job = {
        "placement": {"clusterName": CLUSTER_NAME},
        "pysparkJob": {
            "mainPythonFileUri": INFERENCE_PY,
            "args": [test_path, output_path]
        }
    }

    print(f"Submitting job for {test_path}")
    request = dataproc.projects().regions().jobs().submit(
        projectId=PROJECT_ID,
        region=REGION,
        body={"job": job}
    )

    response = request.execute()
    print(f"Dataproc job submitted: {json.dumps(response)}")
    return response

@functions_framework.cloud_event
def gcs_trigger(event):
    data = event.data
    bucket = data.get("bucket")
    name = data.get("name")

    # Validate file path
    if not name.endswith(".libsvm") or "mnist_test_sampled" not in name or "temporary" in name:
        print("Ignoring unrelated file:", name)
        return

    test_gcs_path = f"gs://{bucket}/{name}"
    output_path = PREDICTIONS_OUT + name.replace("/", "_")

    print(f"Triggering inference for {test_gcs_path}")
    submit_job(test_gcs_path, output_path)
