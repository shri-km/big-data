#!/usr/bin/env python3
"""
longest_lines.py
Download a text file from GCS, find the longest line(s) (by character count),
print results to terminal and upload a results file to GCS.

Usage:
  python3 longest_lines.py --bucket BUCKET_NAME --input INPUT_OBJECT --output OUTPUT_OBJECT
Example:
  python3 longest_lines.py --bucket my-bucket --input input.txt --output results/longest_lines_output.txt
"""

import argparse
import os
import tempfile
from google.cloud import storage

def find_longest_lines(local_path):
    max_len = -1
    results = []  # list of tuples(lineno, line)
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

def main():
    default_bucket = os.environ.get('GCS_BUCKET', '').replace('gs://', '').strip()
    parser = argparse.ArgumentParser(description="Find longest line(s) in a GCS text file.")
    parser.add_argument('--bucket', required=not bool(default_bucket), default=default_bucket or None,
                        help='GCS bucket name (default: GCS_BUCKET env var)')
    parser.add_argument('--input', required=False, default=os.environ.get('W2_INPUT_FILE', 'w2_input_alice.txt'),
                        help='GCS object path for input (default: w2_input_alice.txt or W2_INPUT_FILE env var)')
    parser.add_argument('--output', required=False, default=os.environ.get('W2_OUTPUT_FILE', 'w2_output.txt'),
                        help='GCS object path where results will be uploaded (default: w2_output.txt or W2_OUTPUT_FILE env var)')
    args = parser.parse_args()

    bucket_name = args.bucket.replace('gs://', '').strip('/')
    client = storage.Client()  # use VM credentials
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(args.input)


    with tempfile.TemporaryDirectory() as td:
        local_in = os.path.join(td, 'input.txt')
        print(f"Downloading gs://{bucket_name}/{args.input} -> {local_in} ...")
        try:
            blob.download_to_filename(local_in)
        except Exception as e:
            print(f"ERROR: failed to download object: {e}")
            return 1

        max_len, lines = find_longest_lines(local_in)

        output_lines = []
        output_lines.append(f"Source: gs://{bucket_name}/{args.input}")
        output_lines.append(f"Max length (characters, excluding newline): {max_len}")
        output_lines.append(f"Number of longest lines: {len(lines)}")
        output_lines.append("")
        output_lines.append("Longest line(s):")
        for lineno, line in lines:
            output_lines.append(f"[Line {lineno}] ({len(line)} chars): {line}")

        local_out = os.path.join(td, 'longest_lines_output.txt')
        with open(local_out, 'w', encoding='utf-8') as outf:
            outf.write("\n".join(output_lines))

        print("\n" + "\n".join(output_lines))

        # Upload to GCS
        out_blob = bucket.blob(args.output)
        print(f"\nUploading results to gs://{bucket_name}/{args.output} ...")
        try:
            out_blob.upload_from_filename(local_out)
        except Exception as e:
            print(f"ERROR: failed to upload output: {e}")
            return 1

        print("Finished successfully. Results uploaded to the bucket.")
    return 0

if __name__ == "__main__":
    exit(main())
