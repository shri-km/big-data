#!/usr/bin/env python3
"""
w5_script.py

Usage example:
  spark-submit w5_script.py \
    --customers gs://bucket-name/path/customer_dataset.csv \
    --transactions gs://bucket-name/path/transaction_dataset.csv \
    --output gs://bucket-name/path/output/

"""

import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

def parse_args():
    raw_bucket = os.environ.get("GCS_BUCKET", "gs://your-bucket-name").rstrip("/")
    base_bucket = raw_bucket if raw_bucket.startswith("gs://") else f"gs://{raw_bucket}"

    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", required=False,
                        default=f"{base_bucket}/w5/customer_dataset.csv",
                        help="GCS path to customer CSV (default: $GCS_BUCKET/w5/customer_dataset.csv)")
    parser.add_argument("--transactions", required=False,
                        default=f"{base_bucket}/w5/transaction_dataset.csv",
                        help="GCS path to transaction CSV (default: $GCS_BUCKET/w5/transaction_dataset.csv)")
    parser.add_argument("--output", required=False,
                        default=f"{base_bucket}/w5/",
                        help="GCS output directory (default: $GCS_BUCKET/w5/)")
    parser.add_argument("--low_threshold", type=float, default=2500.0, help="Low threshold for amount_category")
    parser.add_argument("--high_threshold", type=float, default=7500.0, help="High threshold for amount_category")
    args = parser.parse_args()
    return args

def main():
    args = parse_args()

    spark = SparkSession.builder.appName("DataprocCustomerTxnPipeline").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    customers_path = args.customers
    transactions_path = args.transactions
    output_dir = args.output.rstrip("/")

    LOW_THRESHOLD = args.low_threshold
    HIGH_THRESHOLD = args.high_threshold

    CITY_MAP_STANDARD = {
        'Cochin': 'Kochi',
        'Cmbt': 'Coimbatore',
        'Hyd': 'Hyderabad',
        'Trivandrm': 'Trivandrum',
        'Poona': 'Pune'
    }

    # 1. READ CSVs

    print(f"Reading customers from: {customers_path}")
    df_cust = spark.read.option("header", True).option("inferSchema", False).csv(customers_path)
    print("Customer schema:")
    df_cust.printSchema()
    print("Customer sample:")
    df_cust.show(3, truncate=False)

    print(f"Reading transactions from: {transactions_path}")
    df_txn = spark.read.option("header", True).option("inferSchema", False).csv(transactions_path)
    print("Transaction schema:")
    df_txn.printSchema()
    print("Transaction sample:")
    df_txn.show(3, truncate=False)

    # 2. TRANSFORMATIONS

    # Normalize column names

    df_cust = df_cust.toDF(*[c.strip() for c in df_cust.columns])
    df_txn = df_txn.toDF(*[c.strip() for c in df_txn.columns])

    # Standardize city: title-case

    # Create a broadcast map for use in UDF
    bc_city_map = sc.broadcast(CITY_MAP_STANDARD)

    def standardize_city(name):
        if name is None:
            return None
        s = str(name).strip().title()
        return bc_city_map.value.get(s, s)

    # Spark User Defined Function
    standardize_city_udf = F.udf(standardize_city, T.StringType())

    # Standardize transaction_amount to 'double'

    # Keep original column
    df_txn = df_txn.withColumn("transaction_amount_orig", F.col("transaction_amount"))

    # Trim spaces, remove commas, convert to double
    df_txn = df_txn.withColumn("transaction_amount", F.trim(F.col("transaction_amount")))
    df_txn = df_txn.withColumn("transaction_amount", F.when(F.col("transaction_amount").isNull(), None)
                               .otherwise(F.regexp_replace(F.col("transaction_amount"), ",", "")))
    df_txn = df_txn.withColumn("transaction_amount", F.col("transaction_amount").cast(T.DoubleType()))

    # Amount category using when/otherwise (no Python-level UDF needed)
    df_txn = df_txn.withColumn(
        "amount_category",
        F.when(F.col("transaction_amount").isNull() | (F.col("transaction_amount") <= 0), None)
         .when(F.col("transaction_amount") < F.lit(LOW_THRESHOLD), F.lit("Low"))
         .when((F.col("transaction_amount") >= F.lit(LOW_THRESHOLD)) & (F.col("transaction_amount") <= F.lit(HIGH_THRESHOLD)), F.lit("Medium"))
         .otherwise(F.lit("High"))
    )

    # Parse transaction_date to date using multiple patterns (first non-null)
    # Patterns to attempt parse, we can extend if needed, becomes more expensive
    patterns = ["yyyy-MM-dd", "yyyy/MM/dd"]
   
    parsed_cols = [F.to_date(F.col("transaction_date"), p) for p in patterns]

    # coalesce to first non-null parsed date
    df_txn = df_txn.withColumn("transaction_date_parsed", F.coalesce(*parsed_cols))

    # Extract month
    df_txn = df_txn.withColumn("transaction_month", F.month(F.col("transaction_date_parsed")))

    # Standardize city in customers
    if "city" in df_cust.columns:
        df_cust = df_cust.withColumn("city_original", F.col("city"))
        df_cust = df_cust.withColumn("city", standardize_city_udf(F.col("city")))

    # Trim string columns for both DF (apply to all StringType columns)
    def trim_all_string_cols(df):
        for name, dtype in df.dtypes:
            if dtype == "string":
                df = df.withColumn(name, F.trim(F.col(name)))
        return df

    df_cust = trim_all_string_cols(df_cust)
    df_txn = trim_all_string_cols(df_txn)

    print("After transformations - transactions sample:")
    df_txn.select("transaction_id", "customer_id", "transaction_amount", "amount_category",
                   "transaction_date", "transaction_date_parsed", "transaction_month").show(3, truncate=False)

 
    # 3. CLEANING

    # We'll produce: cleaned_customers, cleaned_transactions, invalid_customers, invalid_transactions

    # A. Mark and extract exact duplicates (identical across all columns).

    # Approach: add unique row id, then window partition by all columns, row_number to keep first.
    def mark_duplicates(df, dataset_name):
        cols = df.columns
        # Add unique id for deterministic ordering
        df = df.withColumn("_row_id_tmp", F.monotonically_increasing_id())

        # build partition columns list (all columns except the helper id)
        # We want duplicates across all original columns -> partition by all columns that existed before adding _row_id_tmp
        partition_cols = [F.col(c) for c in cols]
        w = Window.partitionBy(*partition_cols).orderBy(F.col("_row_id_tmp"))
        df = df.withColumn("_dup_row_num", F.row_number().over(w))

        # rows with _dup_row_num == 1 -> keep, others -> invalid duplicates
        cleaned = df.filter(F.col("_dup_row_num") == 1).drop("_dup_row_num", "_row_id_tmp")
        invalids = df.filter(F.col("_dup_row_num") > 1).drop("_dup_row_num", "_row_id_tmp")
        print(f"{dataset_name}: duplicates identified = {invalids.count()} (kept first instance).")
        return cleaned, invalids

    cust_no_dup, cust_dup_invalids = mark_duplicates(df_cust, "Customers")
    txn_no_dup, txn_dup_invalids = mark_duplicates(df_txn, "Transactions")

    # B. Identify invalid customers: missing/blank in key fields such as customer_id, city, status
    # Build mask
    cust_invalid_mask = (
        (F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == "")) |
        (F.col("city").isNull() | (F.trim(F.col("city")) == "")) |
        (F.col("status").isNull() | (F.trim(F.col("status")) == ""))
    )

    cust_invalids_2 = cust_no_dup.filter(cust_invalid_mask)
    cust_cleaned = cust_no_dup.filter(~cust_invalid_mask)
    print(f"Customers: invalid rows from missing key fields = {cust_invalids_2.count()}")

    # Concatenate customer invalids (duplicate invalids + missing-key invalids)
    invalid_customers = cust_dup_invalids.unionByName(cust_invalids_2, allowMissingColumns=True)

    # C. Identify invalid transactions: missing transaction_id or customer_id, non-positive amounts, malformed date
    txn_invalid_mask = (
        F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id")) == "") |
        F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == "") |
        (F.col("transaction_amount").isNull()) | (F.col("transaction_amount") <= 0) |
        F.col("transaction_date_parsed").isNull()
    )

    txn_invalids_2 = txn_no_dup.filter(txn_invalid_mask)
    txn_cleaned = txn_no_dup.filter(~txn_invalid_mask)
    print(f"Transactions: invalid rows from missing keys / non-positive amounts / bad dates = {txn_invalids_2.count()}")

    # Concatenate transaction invalids (duplicate invalids + above invalids)
    invalid_transactions = txn_dup_invalids.unionByName(txn_invalids_2, allowMissingColumns=True)

    # D. Referential integrity: every customer_id in transactions must exist in customers

    # Find transaction rows where customer_id not in cust_cleaned
    cust_ids_df = cust_cleaned.select(F.col("customer_id").alias("_cid")).distinct()
    txn_not_in_cust = txn_cleaned.join(cust_ids_df, txn_cleaned["customer_id"] == cust_ids_df["_cid"], how="left_anti")
    txn_cleaned = txn_cleaned.join(cust_ids_df, txn_cleaned["customer_id"] == cust_ids_df["_cid"], how="left_semi")
    print(f"Transactions: rows failing referential integrity (customer_id not found) = {txn_not_in_cust.count()}")

    invalid_transactions = invalid_transactions.unionByName(txn_not_in_cust, allowMissingColumns=True)

    # E. Re-format dates in cleaned txn to yyyy-MM-dd string column
    txn_cleaned = txn_cleaned.withColumn("transaction_date", F.date_format(F.col("transaction_date_parsed"), "yyyy-MM-dd"))

    print("\n--- Sample invalid customers ---")
    invalid_customers.show(3, truncate=False)
    print("\n--- Sample cleaned customers ---")
    cust_cleaned.show(3, truncate=False)

    print("\n--- Sample invalid transactions ---")
    invalid_transactions.show(3, truncate=False)
    print("\n--- Sample cleaned transactions ---")
    txn_cleaned.select("transaction_id", "customer_id", "transaction_amount", "amount_category", "transaction_date").show(3, truncate=False)

    # 4. JOIN

    joined = txn_cleaned.join(cust_cleaned, on="customer_id", how="inner")
    print(f"Joined row count: {joined.count()}")
    print("Joined sample:")
    joined.show(3, truncate=False)

    # 5. AGGREGATIONS

    # (a) Total & avg per customer
    agg_customer = (
        joined.groupBy("customer_id", "customer_name")
              .agg(
                  F.sum("transaction_amount").alias("total_transaction_amount"),
                  F.avg("transaction_amount").alias("average_transaction_amount"),
                  F.count("transaction_amount").alias("transaction_count")
              )
              .orderBy(F.desc("total_transaction_amount"))
    )

    # (b) Total per city
    if "city" in joined.columns:
        agg_city = (
            joined.groupBy("city")
                  .agg(
                      F.sum("transaction_amount").alias("total_transaction_amount"),
                      F.count("transaction_amount").alias("transaction_count")
                  )
                  .orderBy(F.desc("total_transaction_amount"))
        )
    else:
        agg_city = None

    # (c) Top 3 customers by total_transaction_amount

    top_3_customers = agg_customer.limit(3)

    print("\n--- Aggregation: total & avg per customer (sample) ---")
    agg_customer.show(8, truncate=False)

    if agg_city is not None:
        print("\n--- Aggregation: total per city (sample) ---")
        agg_city.show(8, truncate=False)

    print("\n--- Top 3 customers ---")
    top_3_customers.show(truncate=False)

    # 6. WRITE OUTPUTS to GCS

    out_cleaned_customers = output_dir + "/cleaned_customers"
    out_cleaned_transactions = output_dir + "/cleaned_transactions"
    out_invalid_customers = output_dir + "/invalid_customers"
    out_invalid_transactions = output_dir + "/invalid_transactions"
    out_joined = output_dir + "/joined_data"
    out_agg_customer = output_dir + "/aggregates/agg_customer"
    out_agg_city = output_dir + "/aggregates/agg_city"
    out_top3 = output_dir + "/aggregates/top3_customers"

    cust_cleaned.coalesce(1).write.mode("overwrite").option("header", True).csv(out_cleaned_customers)
    txn_cleaned.coalesce(1).write.mode("overwrite").option("header", True).csv(out_cleaned_transactions)
    invalid_customers.coalesce(1).write.mode("overwrite").option("header", True).csv(out_invalid_customers)
    invalid_transactions.coalesce(1).write.mode("overwrite").option("header", True).csv(out_invalid_transactions)
    joined.coalesce(1).write.mode("overwrite").option("header", True).csv(out_joined)
    agg_customer.coalesce(1).write.mode("overwrite").option("header", True).csv(out_agg_customer)
    if agg_city is not None:
        agg_city.coalesce(1).write.mode("overwrite").option("header", True).csv(out_agg_city)
    top_3_customers.coalesce(1).write.mode("overwrite").option("header", True).csv(out_top3)


    print("\nAll outputs written to:", output_dir)
    print("Output folders created:")
    print(" -", out_cleaned_customers)
    print(" -", out_cleaned_transactions)
    print(" -", out_invalid_customers)
    print(" -", out_invalid_transactions)
    print(" -", out_joined)
    print(" -", out_agg_customer)
    if agg_city is not None:
        print(" -", out_agg_city)
    print(" -", out_top3)

    spark.stop()

if __name__ == "__main__":
    main()