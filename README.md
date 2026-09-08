# Cloud Big Data Engineering & Distributed Systems

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.3%2B-orange.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.1%20(KRaft)-red.svg)](https://kafka.apache.org/)
[![Google Cloud Platform](https://img.shields.io/badge/GCP-Dataproc%20%7C%20Compute%20%7C%20Functions%20%7C%20PubSub-green.svg)](https://cloud.google.com/)

A comprehensive collection of production-grade distributed data pipelines, stream processing systems, event-driven architectures, and distributed machine learning workflows developed across the Big Data Engineering curriculum.

All pipelines are built using **Google Cloud Platform (GCP)** and the **Apache Big Data ecosystem**, incorporating industry best practices such as environment-based configuration, serverless event triggers, fault-tolerant checkpointing, and decoupled pub/sub topologies.

---

## Architecture Overview

```mermaid
flowchart LR
    subgraph Data Sources & Ingestion
        A[Cloud Storage Buckets] --> B[Cloud Functions Ingestion]
        B --> C[Cloud Pub/Sub]
    end

    subgraph Streaming Infrastructure
        D[VM Producer] --> F[Apache Kafka on GCE (KRaft)]
        E[Serverless CF Producer] --> F
    end

    subgraph Distributed Processing Engines
        F --> G[Spark Structured Streaming on Dataproc]
        A --> H[PySpark Batch ETL & SCD Type 1/2]
        A --> I[PySpark MLlib Distributed Training]
    end

    subgraph Consumption & Sinks
        C --> J[Heterogeneous Subscribers (VM, CF, Local)]
        G --> K[Ranked Output Sinks in GCS]
        H --> L[Cleaned Data Warehouse Tables]
        I --> M[Automated Dataproc Inference Trigger]
    end
```

---

## Weekly Curriculum & Project Index

| Week / Exam | Topic / Architecture | Primary Tech Stack | Description & Links |
| :--- | :--- | :--- | :--- |
| **[Week 2](W2/)** | **Compute Engine Text Analytics** | GCE VM, GCS, Python | Automated cloud text analytics on a Compute Engine VM. Streams data from GCS, computes line length statistics, and exports results. [View Details](W2/README.md) |
| **[Week 3](W3/)** | **Serverless Event-Driven Processing** | Cloud Functions (2nd Gen), CloudEvents, GCS | Serverless microservice triggered automatically on GCS file upload. Processes text files and logs structured metrics with loop-prevention heuristics. [View Details](W3/README.md) |
| **[Week 4](W4/)** | **Clickstream Aggregation: RDD vs. DataFrame** | PySpark, Dataproc, GCS | Distributed temporal clickstream aggregation comparing low-level RDD functional transformations with Catalyst-optimized DataFrame operations. [View Details](W4/README.md) |
| **[Week 5](W5/)** | **Production ETL & Referential Integrity** | PySpark DataFrame API, Dataproc | Comprehensive data engineering pipeline: schema normalization, broadcast variable lookups, windowed deduplication, referential integrity (`left_anti`/`left_semi`), and multi-level aggregations. [View Details](W5/README.md) |
| **[Week 6](W6/)** | **Slowly Changing Dimensions (SCD 1 & 2)** | SparkSQL, Dataproc, Data Warehousing | Enterprise dimension lifecycle in pure SparkSQL: implementing both SCD Type 1 (in-place overwrite) and SCD Type 2 (historical record versioning with effective/expiry dates). [View Details](W6/README.md) |
| **[Week 7](W7/)** | **Decoupled Multi-Subscriber Pub/Sub** | GCP Pub/Sub, Cloud Functions, GCE VM | Event-driven fan-out architecture: GCS file upload triggers a Pub/Sub publisher that broadcasts events to three heterogeneous consumers (Cloud Function, GCE VM, and local client). [View Details](W7/README.md) |
| **[Week 8](W8/)** | **Real-Time Streaming Pipeline (Kafka + Spark)** | Apache Kafka 4.x (KRaft), Spark Structured Streaming | End-to-end streaming architecture with ZooKeeper-less Kafka on GCE, dual concurrent producers (VM batch + Serverless CF), and Spark Structured Streaming sliding-window aggregations. [View Details](W8/README.md) |
| **[Week 9](W9/)** | **Distributed Machine Learning & MLOps** | PySpark MLlib, Dataproc, Cloud Functions | Scalable ML pipeline on MNIST (LibSVM): 3-fold cross-validation, hyperparameter grid search (`DecisionTreeClassifier`), model serialization to GCS, and serverless automated batch inference. [View Details](W9/README.md) |
| **[OPPE 1](OPPE/)** | **Large-Scale Railway Transit Analytics** | PySpark, Dataproc, Spark Windowing | Large-scale transit analysis on Indian Railways data: exact percentile estimation via custom linear interpolation, and 1-hour sliding-window event density analysis. [View Details](OPPE/README.md) |
| **[OPPE 2](OPPE2/)** | **Air Quality Streaming & Dynamic Ranking** | Apache Kafka, Spark Structured Streaming | Real-time global air quality streaming: time-series sequence gap detection, carry-forward imputation (LOCF), 8-hour sliding-window multi-pollutant aggregation, and dynamic city ranking. [View Details](OPPE2/) |

---

## Configuration & Environment Setup

All scripts dynamically read cloud identifiers, cluster names, and endpoints from environment variables, falling back to clean defaults when variables are omitted.

Each module directory contains its own self-contained `.env.example` file documenting the exact variables needed for that specific module (e.g., `W7/.env.example`, `W8/.env.example`, `W9/.env.example`).

### 1. Configure Module Environment Variables
Navigate to any module directory and copy its local `.env.example`:
```bash
cd W8
cp .env.example .env
```
Edit `.env` with your specific GCP project, GCS bucket, and cluster identifiers:
```bash
# Example for Week 8 (Kafka + Spark Streaming)
KAFKA_BROKER="10.160.15.206:9092"
KAFKA_TOPIC="streaming-data"
GCS_BUCKET="your-gcs-bucket-name"
PRODUCER2_CF_URL="https://producer2-cf-xxxx.asia-south1.run.app"
```

### 2. Running a PySpark Job on Dataproc
To submit any pipeline to your Dataproc cluster:
```bash
gcloud dataproc jobs submit pyspark W5/w5_script.py \
    --cluster=$DATAPROC_CLUSTER_NAME \
    --region=$GCP_REGION \
    -- \
    --customers="gs://$GCS_BUCKET/w5/customer_dataset.csv" \
    --transactions="gs://$GCS_BUCKET/w5/transaction_dataset.csv" \
    --output="gs://$GCS_BUCKET/w5/output/"
```

### 3. Setting Up Apache Kafka (KRaft Mode)
To initialize Kafka on a Google Compute Engine VM without ZooKeeper:
```bash
cd W8/kafka_broker_init
chmod +x *.sh
./1_setup_kafka.sh
./2_config_and_start.sh
./3_create_topic.sh
```

---

## Repository Structure

```text
├── .gitignore                  # Git ignore file (videos, zips, secrets, caches)
├── README.md                   # Global project documentation
├── W2/                         # Week 2: Compute Engine VM Text Analytics (.env.example)
├── W3/                         # Week 3: Serverless GCS Cloud Functions
├── W4/                         # Week 4: PySpark RDD vs. DataFrame Benchmarks (.env.example)
├── W5/                         # Week 5: Production ETL, Deduplication, Integrity (.env.example)
├── W6/                         # Week 6: Slowly Changing Dimensions in SparkSQL (.env.example)
├── W7/                         # Week 7: Decoupled Multi-Subscriber Pub/Sub (.env.example)
├── W8/                         # Week 8: End-to-End Real-Time Kafka & Spark Streaming (.env.example)
├── W9/                         # Week 9: Distributed MLlib & Serverless MLOps (.env.example)
├── OPPE/                       # OPPE 1: Large-Scale Transit Analytics (.env.example)
└── OPPE2/                      # OPPE 2: Real-Time Air Quality Streaming & Ranking (.env.example)
```

---
