import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# =====================
# CONFIG (reads from environment variables with defaults)
# =====================
_bucket = os.environ.get("GCS_BUCKET", "your-gcs-bucket-name").replace("gs://", "").strip("/")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BROKER", "10.160.15.206:9092")
TOPIC = os.environ.get("AQ_KAFKA_TOPIC", "aqdata")

CHECKPOINT = os.environ.get("OPPE2_CHECKPOINT", f"gs://{_bucket}/oppe2/checkpoints/aq_ranking")
OUTPUT_PATH = os.environ.get("OPPE2_OUTPUT_PATH", f"gs://{_bucket}/oppe2/output/aq_city_rankings")

# =====================
# Spark Session
# =====================
spark = SparkSession.builder \
    .appName("AQ-Streaming-Ranking") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "8")

# =====================
# Logger
# =====================
log4j = spark._jvm.org.apache.log4j
logger = log4j.LogManager.getLogger("AQ-Streaming-Ranking")
logger.info("Starting AQ Streaming Consumer")

# =====================
# Schema (must match producer)
# =====================
schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("pm25", DoubleType()),
    StructField("pm10", DoubleType()),
    StructField("no2", DoubleType()),
    StructField("so2", DoubleType()),
    StructField("o3", DoubleType()),
    StructField("co", DoubleType()),   # mg/m3
    StructField("aqi", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("wind_speed", DoubleType())
])

# =====================
# Read Kafka Stream
# =====================
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

logger.info("Kafka stream initialized")

# =====================
# Parse JSON
# =====================
df = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# =====================
# Watermark
# =====================
df = df.withWatermark("event_time", "1 hour")

# =====================
# Unit Conversion → ppb
# =====================
df = df.withColumn("pm25_ppb", col("pm25")) \
       .withColumn("pm10_ppb", col("pm10")) \
       .withColumn("no2_ppb", col("no2") / 1.88) \
       .withColumn("so2_ppb", col("so2") / 2.62) \
       .withColumn("o3_ppb", col("o3") / 2.0) \
       .withColumn("co_ppb", (col("co") * 1000) / 1.145)

# =====================
# 8-Hour Sliding Window Aggregation
# =====================
agg = df.groupBy(
    window(col("event_time"), "8 hours", "1 hour"),
    col("city")
).agg(
    max("aqi").alias("V1"),
    (
        sum("pm25_ppb") +
        sum("pm10_ppb") +
        sum("no2_ppb") +
        sum("so2_ppb") +
        sum("o3_ppb") +
        sum("co_ppb")
    ).alias("V2")
)

# =====================
# foreachBatch: Rank + Write CSV
# =====================
def rank_and_write(batch_df, batch_id):

    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id} empty — skipping")
        return

    logger.info(f"Processing batch {batch_id}")

    rank_window = Window.partitionBy("window") \
                        .orderBy(col("V1").asc(), col("V2").asc())

    ranked = batch_df.withColumn("rank", row_number().over(rank_window))

    output = ranked.select(
        col("window.start").alias("hour"),
        "rank",
        "city",
        "V1",
        "V2"
    ).orderBy("hour", "rank")

    # Console output
    output.show(truncate=False)

    # Write CSV to GCS
    output \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"{OUTPUT_PATH}/batch_{batch_id}")

# =====================
# Start Streaming Query
# =====================
query = agg.writeStream \
    .foreachBatch(rank_and_write) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT) \
    .start()

query.awaitTermination()
