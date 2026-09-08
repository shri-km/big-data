import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

def main():

    spark = SparkSession.builder.appName("OPPE").getOrCreate()
    sc = spark.sparkContext

    _bucket = os.environ.get("GCS_BUCKET", "your-gcs-bucket-name").replace("gs://", "").strip("/")
    input_path = os.environ.get("OPPE_INPUT_PATH", f'gs://{_bucket}/oppe/Train_details_22122017.csv')

    ### Section A

    ## Read dataset
    print(f"Reading input from: {input_path}")
    df_raw = spark.read.option("header", True).csv(input_path)
    print("Input schema:")
    df_raw.printSchema()

    ## Drop Missing/Bad Rows
    df = df_raw.na.drop()

    ## Convert Arrival and Departure dates to timestamp
    df = df.withColumn("arrival", F.to_timestamp(F.col("Arrival Time"), "HH:mm:ss"))
    df = df.withColumn("departure", F.to_timestamp(F.col("Departure Time"), "HH:mm:ss"))

    ## Calculate stop duration in minutes for each train
    df = df.withColumn("duration_minutes", F.abs(F.col("arrival").cast("long") - F.col("departure").cast("long")) / 60)

    ### Section B - Analysis

    # For each station,
    df.repartition("Station Name")
    agg = (
        df.groupBy("Station Name")
                .agg(
                    F.mean("duration_minutes").alias("mean_duration"),
                    F.stddev("duration_minutes").alias("std_duration"),
                    F.sum("duration_minutes").alias("total_duration"),
                    F.count("duration_minutes").alias("count_duration"),
                    F.collect_list("duration_minutes").alias("vals")
                )
    )

    agg = agg.withColumn("sorted_vals", F.sort_array("vals")) \
            .withColumn("n", F.size("sorted_vals"))

    # Compute an exact percentile column expression for probability p (0..1)
    def percentile_expr(sorted_col, n_col, p):
        # position using (n-1)*p (0-based)
        pos = ((F.col(n_col) - F.lit(1.0)) * F.lit(float(p))).alias("pos")
        floor_pos = F.floor(pos).cast("int")  # 0-based floor index
        ceil_pos = F.ceil(pos).cast("int")    # 0-based ceil index
        # Convert to 1-based indices for element_at
        lower_idx_1b = (floor_pos + 1)
        upper_idx_1b = (ceil_pos + 1)
        frac = (pos - F.floor(pos)).cast("double")  # fractional part for interpolation

        lower_val = F.element_at(F.col(sorted_col), lower_idx_1b)
        upper_val = F.element_at(F.col(sorted_col), upper_idx_1b)

        # if n == 0 -> null
        # if floor_pos == ceil_pos -> exact element; else linear interpolation
        interp = F.when(
            F.col(n_col) == 0, None
        ).when(
            lower_idx_1b == upper_idx_1b, lower_val
        ).otherwise(
            (F.lit(1.0) - frac) * lower_val + frac * upper_val
        )

        return interp

    # Median (p=0.5) and percentiles
    agg = agg.withColumn("median_duration_exact", percentile_expr("sorted_vals", "n", 0.5)) \
            .withColumn("p95_duration_exact", percentile_expr("sorted_vals", "n", 0.95)) \
            .withColumn("p99_duration_exact", percentile_expr("sorted_vals", "n", 0.99))

    df_stats = agg.select(
        F.col("Station Name"),
        F.round(F.col("mean_duration"), 6).alias('mean_duration'),
        F.round(F.col("std_duration"), 6).alias('std_duration'),
        F.round(F.col("median_duration_exact"), 6).alias("median_duration"),
        F.round(F.col("p95_duration_exact"), 6).alias("p95_duration"),
        F.round(F.col("p99_duration_exact"), 6).alias("p99_duration"),
        F.round(F.col("total_duration"), 6).alias("total_duration"),
        F.col("count_duration").alias("n")
    )

    df_stats.show(7, truncate=False)

    busy5 = df_stats.orderBy(F.desc("total_duration")).limit(5)
    print("Top 5 busiest stations:")
    busy5.show(truncate=False)

    top1_row = busy5.limit(1).collect()

    if top1_row:
        busiest_station = top1_row[0]["Station Name"]
        print("Computing 1-hour sliding-window max for station:", busiest_station)

        station_events = df.filter(F.col("Station Name") == busiest_station) \
                        .select("Station Name", "departure") \
                        .filter(F.col("departure").isNotNull())

        # define window ordered by departure cast to long seconds; rangeBetween uses numeric units of ORDER BY expression (seconds)
        # NB: rangeBetween needs a deterministic numeric order col. We use departure cast to long (seconds).
        order_col = F.col("departure").cast("long")
        w = Window.partitionBy("Station Name").orderBy(order_col).rangeBetween(-3600, 0)  # last 3600 seconds (1 hour) inclusive

        # count rows in the 1-hour window ending at each event
        station_counts = station_events.withColumn("count_1h", F.count("*").over(w))

        # get the maximum count across events (this is the max trains in any 1-hour sliding window)
        max_count_row = station_counts.select(F.max("count_1h").alias("max_trains_1h")).collect()
        max_trains_1h = max_count_row[0]["max_trains_1h"] if max_count_row else None
        print(f"Max trains within any 1-hour window for {busiest_station}: {max_trains_1h}")
    else:
        print("No stations found in busy5.")
        
    gcs_output_path = os.environ.get("OPPE_OUTPUT_PATH", f"gs://{_bucket}/oppe/station_stats")

    df_stats_coalesced = df_stats.coalesce(1)
    df_stats_coalesced.write.mode("overwrite").option("header", "true").csv(gcs_output_path)

    print("Saved station stats to:", gcs_output_path)

if __name__ == "__main__":
    main()