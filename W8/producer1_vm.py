import os
import sys
import csv
import json
import time
from kafka import KafkaProducer
from google.cloud import storage

# === CONFIG (reads from environment variables with defaults) ===
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', '10.160.15.206:9092')
TOPIC_NAME = os.environ.get('KAFKA_TOPIC', 'streaming-data')
BUCKET_NAME = os.environ.get('GCS_BUCKET', 'your-gcs-bucket-name').replace('gs://', '').strip('/')
FILE_NAME = os.environ.get('W8_INPUT_FILE_1', 'w8/input/input_file_1.csv')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '10'))
SLEEP_INTERVAL = int(os.environ.get('SLEEP_INTERVAL', '10'))  # in seconds
TOTAL_RECORDS = int(os.environ.get('TOTAL_RECORDS', '1000'))
# ===============================================================

def get_file_from_gcs(bucket_name, file_name):
    """Download and parse CSV from GCS"""

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        print(f"Downloading {file_name} from bucket {bucket_name}...")
        content = blob.download_as_text()
        
        lines = content.strip().split('\n')
        reader = csv.DictReader(lines)
        data = list(reader)
        print(f"Loaded {len(data)} records from GCS")
        return data
    except Exception as e:
        print(f"Error loading file from GCS: {e}")
        sys.exit(1)

def create_kafka_producer(broker):
    """Create and configure Kafka producer"""

    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,
            api_version=(3, 0, 0)  # Kafka 3.x+ / KRaft compatible
        )
        print(f"Connected to Kafka broker: {broker}")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        sys.exit(1)

def main():
    print("=" * 60)
    print("PRODUCER 1 - VM Based")
    print("=" * 60)
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Batch Size: {BATCH_SIZE} records")
    print(f"Interval: {SLEEP_INTERVAL} seconds")
    print(f"Total Records: {TOTAL_RECORDS}")
    print("=" * 60)
    
    # Load data from GCS
    data = get_file_from_gcs(BUCKET_NAME, FILE_NAME)
    
    # Create Kafka producer
    producer = create_kafka_producer(KAFKA_BROKER)
    
    # Send data in batches
    records_sent = 0
    batch_number = 0
    start_time = time.time()
    
    try:
        while records_sent < TOTAL_RECORDS:
            batch_number += 1
            batch_start = records_sent
            batch_end = min(records_sent + BATCH_SIZE, TOTAL_RECORDS)
            
            print(f"\n{'='*60}")
            print(f"BATCH {batch_number}")
            print(f"{'='*60}")
            print(f"Sending records {batch_start} to {batch_end-1}...")
            
            batch_success = 0
            for i in range(batch_start, batch_end):
                # Get record (loop if we run out of data)
                record = data[i % len(data)].copy()
                
                # Add metadata
                record['producer_id'] = 'producer_1'
                record['record_number'] = i
                record['send_timestamp'] = time.time()
                
                # Send to Kafka
                try:
                    future = producer.send(
                        TOPIC_NAME,
                        key=f"p1_{i}",
                        value=record
                    )
                    
                    # Wait for acknowledgment
                    metadata = future.get(timeout=10)
                    batch_success += 1
                    
                    if i % 5 == 0 or i == batch_end - 1:
                        print(f"  Record {i}: partition={metadata.partition}, "
                              f"offset={metadata.offset}")
                    
                except Exception as e:
                    print(f"Failed to send record {i}: {e}")
            
            records_sent = batch_end
            elapsed = time.time() - start_time
            
            print(f"\nBatch Summary:")
            print(f"  Sent: {batch_success}/{BATCH_SIZE} records")
            print(f"  Total Progress: {records_sent}/{TOTAL_RECORDS}")
            print(f"  Elapsed Time: {elapsed:.1f}s")
            
            # Sleep between batches
            if records_sent < TOTAL_RECORDS:
                print(f"\nSleeping for {SLEEP_INTERVAL} seconds...")
                time.sleep(SLEEP_INTERVAL)
        
        # Final summary
        total_time = time.time() - start_time
        print(f"\n{'='*60}")
        print("PRODUCER 1 COMPLETED")
        print(f"{'='*60}")
        print(f"Total Records Sent: {records_sent}")
        print(f"Total Time: {total_time:.1f}s")
        print(f"Average Rate: {records_sent/total_time:.2f} records/sec")
        print(f"{'='*60}")
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed")

if __name__ == "__main__":
    main()
