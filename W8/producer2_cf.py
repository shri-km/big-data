import os
import json
import time
import csv
from kafka import KafkaProducer
from google.cloud import storage
import functions_framework

### Requirements.txt
### google-cloud-storage kafka-python

# === CONFIG (reads from environment variables with defaults) ===
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', '10.160.15.206:9092')  # Kafka VM INTERNAL IP
TOPIC_NAME = os.environ.get('KAFKA_TOPIC', 'streaming-data')
BUCKET_NAME = os.environ.get('GCS_BUCKET', 'your-gcs-bucket-name').replace('gs://', '').strip('/')
FILE_NAME = os.environ.get('W8_INPUT_FILE_2', 'w8/input/input_file_2.csv')
STATE_FILE = os.environ.get('W8_STATE_FILE', 'w8/producer2_state.json')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '5'))
TOTAL_RECORDS = int(os.environ.get('TOTAL_RECORDS', '1000'))
# ===============================================================

def get_file_from_gcs(bucket_name, file_name):
    """Download CSV from GCS"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()
    lines = content.strip().split('\n')
    return list(csv.DictReader(lines))

def get_state(bucket_name):
    """Load current progress state"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(STATE_FILE)
    
    try:
        if blob.exists():
            content = blob.download_as_text()
            return json.loads(content)
    except:
        pass
    
    return {'records_sent': 0, 'completed': False, 'batches': 0}

def save_state(bucket_name, state):
    """Save progress state"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(STATE_FILE)
    blob.upload_from_string(
        json.dumps(state),
        content_type='application/json'
    )

def create_kafka_producer(broker):
    """Create Kafka producer"""
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        request_timeout_ms=30000,
        api_version=(3, 0, 0)
    )
    return producer

@functions_framework.http
def produce_batch(request):
    """Cloud Function entry point - sends one batch"""
    
    start_time = time.time()
    
    # Get current state
    state = get_state(BUCKET_NAME)
    
    if state['completed']:
        return json.dumps({
            'status': 'completed',
            'message': 'All 1000 records already sent',
            'total_sent': state['records_sent']
        }), 200
    
    records_sent = state['records_sent']
    batches = state.get('batches', 0)
    
    print(f"Producer 2 - Batch {batches + 1}")
    print(f"Current progress: {records_sent}/{TOTAL_RECORDS}")
    
    try:
        # Create producer
        producer = create_kafka_producer(KAFKA_BROKER)
        
        # Load data
        data = get_file_from_gcs(BUCKET_NAME, FILE_NAME)
        
        # Calculate batch range
        batch_start = records_sent
        batch_end = min(records_sent + BATCH_SIZE, TOTAL_RECORDS)
        
        print(f"Sending records {batch_start} to {batch_end-1}")
        
        # Send batch
        success_count = 0
        for i in range(batch_start, batch_end):
            record = data[i % len(data)].copy()
            record['producer_id'] = 'producer_2'
            record['record_number'] = i
            record['send_timestamp'] = time.time()
            
            try:
                future = producer.send(
                    TOPIC_NAME,
                    key=f"p2_{i}",
                    value=record
                )
                metadata = future.get(timeout=10)
                print(f"  Sent record {i}: partition={metadata.partition}, "
                      f"offset={metadata.offset}")
                success_count += 1
            except Exception as e:
                print(f"  Failed record {i}: {e}")
        
        producer.flush()
        producer.close()
        
        # Update state
        new_records_sent = batch_end
        completed = new_records_sent >= TOTAL_RECORDS
        
        save_state(BUCKET_NAME, {
            'records_sent': new_records_sent,
            'completed': completed,
            'batches': batches + 1
        })
        
        elapsed = time.time() - start_time
        
        response = {
            'status': 'success' if not completed else 'completed',
            'batch_number': batches + 1,
            'batch_sent': success_count,
            'total_sent': new_records_sent,
            'remaining': TOTAL_RECORDS - new_records_sent,
            'execution_time': f"{elapsed:.2f}s"
        }
        
        print(f"Batch complete: {response}")
        return json.dumps(response), 200
        
    except Exception as e:
        error_msg = f"Error in Cloud Function: {str(e)}"
        print(error_msg)
        return json.dumps({
            'status': 'error',
            'message': error_msg
        }), 500
