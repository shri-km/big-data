import os
import requests
import time
import json

# === CONFIG (reads from environment variables with defaults) ===
CLOUD_FUNCTION_URL = os.environ.get('PRODUCER2_CF_URL', 'https://producer2-cf-449221341237.asia-south1.run.app')
TOTAL_INVOCATIONS = int(os.environ.get('TOTAL_INVOCATIONS', '200'))  # 1000 records / 5 per batch
SLEEP_INTERVAL = int(os.environ.get('SLEEP_INTERVAL', '5'))
# ==============================================================

def main():
    print("=" * 60)
    print("PRODUCER 2 TRIGGER - Cloud Function Invoker")
    print("=" * 60)
    print(f"Function URL: {CLOUD_FUNCTION_URL}")
    print(f"Total Invocations: {TOTAL_INVOCATIONS}")
    print(f"Interval: {SLEEP_INTERVAL} seconds")
    print("=" * 60)
    
    start_time = time.time()
    successful = 0
    failed = 0
    
    for i in range(TOTAL_INVOCATIONS):
        print(f"\n--- Invocation {i+1}/{TOTAL_INVOCATIONS} ---")
        
        try:
            response = requests.get(CLOUD_FUNCTION_URL, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                print(f"Status: {result.get('status')}")
                print(f"Batch Sent: {result.get('batch_sent')}")
                print(f"Total Sent: {result.get('total_sent')}")
                print(f"Remaining: {result.get('remaining')}")
                
                successful += 1
                
                if result.get('status') == 'completed':
                    print("\n✓ All records sent. Stopping.")
                    break
            else:
                print(f"✗ HTTP Error: {response.status_code}")
                print(f"Response: {response.text}")
                failed += 1
                
        except Exception as e:
            print(f"✗ Error: {e}")
            failed += 1
        
        if i < TOTAL_INVOCATIONS - 1:
            print(f"Sleeping {SLEEP_INTERVAL} seconds...")
            time.sleep(SLEEP_INTERVAL)
    
    # Summary
    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print("TRIGGER COMPLETED")
    print(f"{'='*60}")
    print(f"Successful invocations: {successful}")
    print(f"Failed invocations: {failed}")
    print(f"Total time: {total_time:.1f}s")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
