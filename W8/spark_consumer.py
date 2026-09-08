import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count, col

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "10.160.15.206:9092")  # Kafka broker host:port
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "streaming-data")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "5 seconds")      # Micro-batch interval
WINDOW_LENGTH = os.environ.get("WINDOW_LENGTH", "10 seconds")       # Window length for the count

# -- Start Spark session --
spark = SparkSession.builder \
    .appName("SparkRowCount10sec") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -- Read from Kafka --
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Use processing timestamp for windowing
msg_df = kafka_df.select(col("timestamp"))

# -- Window aggregation: Count rows seen in last 10 seconds --
windowed_counts = msg_df \
    .withWatermark("timestamp", WINDOW_LENGTH) \
    .groupBy(window(col("timestamp"), WINDOW_LENGTH)) \
    .agg(count("*").alias("row_count")) \
    .select(col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("row_count"))

# -- Console output: don't sort, just print each window --
query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 100) \
    .option("checkpointLocation", f"/tmp/kafka_chk_{uuid.uuid4()}") \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .start()

print("\nStreaming active! Count of rows in last 10s will appear every 5s. Stop with Ctrl+C.\n")
query.awaitTermination()