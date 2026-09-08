# Big Data - Week 5 Assignment

## Overview

This assignment uses Google Cloud Dataproc and PySpark to perform a complete data engineering workflow on customer and transaction datasets stored in Google Cloud Storage (GCS).

The goal is to:

* Clean and standardize raw data,

* Remove invalid and duplicate records,

* Ensure referential integrity between customers and transactions,

* Join both datasets,

* And compute several key aggregations such as total and average transaction amounts per customer and per city.

* All processing is done using the PySpark DataFrame API on a Dataproc cluster, with GCS serving as both the input and output storage system.

---

## Files in this Submission

### 1. Script - `W4GA_RDD.ipynb`

* **Purpose:**  
  Main PySpark script executed on the Dataproc cluster. It performs all the data transformations, cleaning, joining, and aggregation tasks.

* **What it does:**  
  1. Reads Customer and Transaction datasets from GCS.
  2. Inspects and logs sample data and schemas.
  3. Standardizes text columns and city names.
  4. Parses and validates transaction dates.
  5. Classifies transactions as Low, Medium, or High based on amount thresholds.
  6. Identifies and removes invalid or duplicate rows.
  7. Ensures that every transaction’s customer_id exists in the Customer dataset (referential integrity).
  8. Joins the cleaned datasets on customer_id.
  9. Computes and saves aggregate summaries to GCS:
  10. Total and average transaction amount per customer
  11. Total transaction amount per city
  12. Top 3 customers by total transaction amount
  13. Logs intermediate sample outputs in the driver logs for verification.

---

### Output Files-

8 output files obtained by running the job.

---

## Pipeline

1. Upload both datasets to GCS Bucket
2. Create and start a Dataproc Cluster
3. Submit Pyspark Job as shown in the video demo
4. Output files are saved back to GCS

## Learnings

Understood how to use Dataproc with PySpark for scalable data processing.

Learned systematic handling of invalid and duplicate records.

Gained practical experience in data standardization and referential integrity checks.

Strengthened understanding of PySpark DataFrame transformations for production-style pipelines.

Practiced running and monitoring Spark jobs on Google Cloud.
