import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# CONFIG (reads from environment variables with defaults)
_bucket = os.environ.get("GCS_BUCKET", "your-gcs-bucket-name").replace("gs://", "").strip("/")
GCS_PATH = os.environ.get("OPPE2_GCS_PATH", f"gs://{_bucket}/oppe2/globalAirQuality.csv")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BROKER", "10.160.15.206:9092")
TOPIC = os.environ.get("AQ_KAFKA_TOPIC", "aqdata")

def main():
    spark = SparkSession.builder \
        .appName("AQ-Batch-Producer") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Schema
    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("country", StringType()),
        StructField("city", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("pm25", DoubleType()),
        StructField("pm10", DoubleType()),
        StructField("no2", DoubleType()),
        StructField("so2", DoubleType()),
        StructField("o3", DoubleType()),
        StructField("co", DoubleType()),
        StructField("aqi", IntegerType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("wind_speed", DoubleType())
    ])

    # Read
    print('Reading Data')
    df = spark.read.csv(
        GCS_PATH,
        header=True,
        schema=schema,
        mode="PERMISSIVE"
    )

    # Cleaning bad rows
    print('Starting Cleaning')
    df = df.dropna(subset=["timestamp", "city", "aqi"])

    df = df.withColumn(
        "event_hour",
        F.date_trunc("hour", F.to_timestamp("timestamp"))
    ).filter(F.col("event_hour").isNotNull()) \
     .drop("timestamp")

    # Handle unsorted input
    df = df.orderBy("city", "event_hour")

    # Detect missing hours
    print('Detecting missing hours')
    city_window = Window.partitionBy("city").orderBy("event_hour")

    df = df.withColumn(
        "prev_hour",
        F.lag("event_hour").over(city_window)
    )

    df = df.withColumn(
        "missing_hours",
        F.when(
            F.col("prev_hour").isNotNull() &
            ((F.col("event_hour").cast("long") -
              F.col("prev_hour").cast("long")) > 3600),
            F.expr(
                "sequence(prev_hour + interval 1 hour, "
                "event_hour - interval 1 hour, interval 1 hour)"
            )
        ).otherwise(F.array())
    )

    # Generate rows for missing hours (values initially null)
    filled = df.select(
        F.explode_outer("missing_hours").alias("event_hour"),
        "city",
        F.lit(None).cast(StringType()).alias("country"),
        F.lit(None).cast(DoubleType()).alias("latitude"),
        F.lit(None).cast(DoubleType()).alias("longitude"),
        F.lit(None).cast(DoubleType()).alias("pm25"),
        F.lit(None).cast(DoubleType()).alias("pm10"),
        F.lit(None).cast(DoubleType()).alias("no2"),
        F.lit(None).cast(DoubleType()).alias("so2"),
        F.lit(None).cast(DoubleType()).alias("o3"),
        F.lit(None).cast(DoubleType()).alias("co"),
        F.lit(None).cast(IntegerType()).alias("aqi"),
        F.lit(None).cast(DoubleType()).alias("temperature"),
        F.lit(None).cast(DoubleType()).alias("humidity"),
        F.lit(None).cast(DoubleType()).alias("wind_speed")
    ).filter(F.col("event_hour").isNotNull())

    original = df.select(
        "event_hour", "city", "country", "latitude", "longitude",
        "pm25", "pm10", "no2", "so2", "o3", "co",
        "aqi", "temperature", "humidity", "wind_speed"
    )

    combined = original.unionByName(filled)

    # Carry-forward imputation
    carry_window = Window.partitionBy("city") \
                         .orderBy("event_hour") \
                         .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    for c in ["pm25", "pm10", "no2", "so2", "o3", "co",
              "aqi", "temperature", "humidity", "wind_speed",
              "country", "latitude", "longitude"]:
        combined = combined.withColumn(
            c,
            F.last(c, ignorenulls=True).over(carry_window)
        )

    #  Deduplicate for safety 
    final_df = combined.dropDuplicates(["city", "event_hour"]) \
                       .orderBy("event_hour", "city")

    # Kafka payload
    kafka_df = final_df.selectExpr(
        "CAST(city AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    # Write to Kafka
    print('Writing to Kafka')
    kafka_df.coalesce(1) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", TOPIC) \
        .save()

    spark.stop()


if __name__ == "__main__":
    main()