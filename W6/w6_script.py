import os
import sys
from pyspark.sql import SparkSession

raw_bucket = os.environ.get("GCS_BUCKET", "gs://your-bucket-name").rstrip("/")
base_bucket = raw_bucket if raw_bucket.startswith("gs://") else f"gs://{raw_bucket}"
GCS_BUCKET = os.environ.get("W6_GCS_BUCKET", f"{base_bucket}/w6")
INPUT_PATH = f"{GCS_BUCKET}/input"
OUTPUT_PATH = f"{GCS_BUCKET}/output"

spark = SparkSession.builder.appName("W6_GA").getOrCreate()

SHOW_ROWS = 20

def run_spark_sql_job():

    print("\n--- Loading Data ---")
    try:
        spark.read.option("header", "true").csv(f"{INPUT_PATH}/transactions.csv").createOrReplaceTempView("transactions_raw")
        spark.read.option("header", "true").csv(f"{INPUT_PATH}/customer_master.csv").createOrReplaceTempView("customer_master_initial_raw")
        spark.read.option("header", "true").csv(f"{INPUT_PATH}/customer_updates.csv").createOrReplaceTempView("customer_updates_raw")
        print("All input data loaded.")

    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)

    print("\n\n### Aggregation Queries ###")

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW transactions AS
    SELECT
        customer_id,
        CAST(transaction_amount AS DOUBLE) AS transaction_amount,
        transaction_date
    FROM transactions_raw
    """)

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW customer_master_initial AS
    SELECT
        customer_id,
        customer_name,
        city,
        dob,
        CAST(effective_date AS DATE) AS effective_date,
        CAST(expiry_date AS DATE) AS expiry_date,
        CAST(current_flag AS INT) AS current_flag
    FROM customer_master_initial_raw
    """)

    spark.sql("""
    CREATE OR REPLACE TEMP VIEW customer_updates AS
    SELECT
        customer_id,
        customer_name,
        city,
        dob,
        CAST(change_date AS DATE) AS change_date
    FROM customer_updates_raw
    """)
    
    # Q1: Top 5 customers by total transaction amount
    query_top_5 = """
    SELECT customer_id, CAST(SUM(transaction_amount) AS DECIMAL(10, 2)) AS total_amount
    FROM transactions 
    GROUP BY customer_id 
    ORDER BY total_amount DESC 
    LIMIT 5
    """
    df_top_5 = spark.sql(query_top_5)
    print("\n--- Top 5 Customers by Total Transaction Amount ---")
    df_top_5.show()
    df_top_5.write.mode("overwrite").csv(f"{OUTPUT_PATH}/top5_customers", header=True)

    # Q2: Number of customers per city
    query_customers_per_city = """
    SELECT city, COUNT(DISTINCT customer_id) AS customer_count
    FROM customer_master_initial 
    WHERE current_flag = 1 
    GROUP BY city
    """
    df_customers_per_city = spark.sql(query_customers_per_city)
    print("\n--- Number of Customers per City (Current Master) ---")
    df_customers_per_city.show()
    df_customers_per_city.write.mode("overwrite").csv(f"{OUTPUT_PATH}/customers_per_city", header=True)

    # Q3: Average transaction value overall
    query_avg_txn = """
    SELECT CAST(AVG(transaction_amount) AS DECIMAL(10, 2)) AS average_transaction_value 
    FROM transactions
    """
    df_avg_txn = spark.sql(query_avg_txn)
    print("\n--- Average Transaction Value Overall ---")
    df_avg_txn.show()
    df_avg_txn.write.mode("overwrite").csv(f"{OUTPUT_PATH}/average_transaction_value", header=True)

    # --- 3. Slowly Changing Dimensions (SCD) ---

    print("\n\n### Slowly Changing Dimensions (SCD) ###")
    
    # --- 3a. SCD Type I (Overwrite) ---
    print("\n*** 3a. SCD Type I (Overwrite) ***")
    
    # 1. Prepare for SCD I by filtering to active customers (only active rows are relevant for SCD I)
    spark.sql("SELECT * FROM customer_master_initial WHERE current_flag = 1").createOrReplaceTempView("customer_master_scd1")
    
    print(f"\n--- Initial State (Customer Master SCD I - Active Rows) ---")
    spark.sql("SELECT * FROM customer_master_scd1").show(SHOW_ROWS, truncate=False)
    
    # 2. SCD Type I Logic (Replace the master record with the update where IDs match)
    query_scd1_final = f"""
    -- Use LEFT JOIN so updates for customers not in current master are included (they become new rows)

    WITH CombinedData AS (
    SELECT
        cu.customer_id,
        cu.customer_name,
        cu.city,
        cu.dob,

        -- if master has an effective_date use it, otherwise fall back to update change_date (or CURRENT_DATE())
        COALESCE(cm.effective_date, cu.change_date) AS effective_date,
        cm.expiry_date,
        COALESCE(cm.current_flag, 1) AS current_flag

    FROM customer_updates cu
    LEFT JOIN customer_master_scd1 cm
        ON cu.customer_id = cm.customer_id

    UNION ALL

    -- Unaffected current master rows
    SELECT cm.*
    FROM customer_master_scd1 cm
    LEFT ANTI JOIN customer_updates cu
        ON cm.customer_id = cu.customer_id
    )
    SELECT * FROM CombinedData
    """
    df_scd1_final = spark.sql(query_scd1_final)
    df_scd1_final.createOrReplaceTempView("customer_master_scd1_final")

    print(f"\n--- Final State (Customer Master SCD I) ---")
    spark.sql("SELECT customer_id, customer_name, city, dob FROM customer_master_scd1_final LIMIT 10").show(SHOW_ROWS, truncate=False)
    
    df_scd1_final.write.mode("overwrite").csv(f"{OUTPUT_PATH}/customer_master_scd1_final", header=True)
    print(f"Final SCD I table saved to {OUTPUT_PATH}/customer_master_scd1_final.csv")
    
    
    # --- 3b. SCD Type II (Add New Row) ---
    print("\n\n*** 3b. SCD Type II (Add New Row/Full History) ***")
    
    # Use the customer_master_initial view directly for SCD II logic
    spark.sql("SELECT * FROM customer_master_initial").createOrReplaceTempView("customer_master_scd2")
    
    print(f"\n--- Initial State (Customer Master SCD II - Full Table Sample) ---")
    spark.sql("""
        SELECT customer_id, customer_name, city, effective_date, expiry_date, current_flag 
        FROM customer_master_scd2 
        ORDER BY customer_id, current_flag DESC -- Optional
    """).show(SHOW_ROWS, truncate=False)

    # Step A: Invalidate the old records
    query_invalidate = f"""
    SELECT
        cm.customer_id, cm.customer_name, cm.city, cm.dob, cm.effective_date,
        cu.change_date AS expiry_date,
        0 AS current_flag
    FROM customer_master_scd2 cm
    INNER JOIN customer_updates cu ON cm.customer_id = cu.customer_id
    WHERE cm.current_flag = 1
    """
    spark.sql(query_invalidate).createOrReplaceTempView("invalidated_rows")
    
    # Step B: Insert the New records
    query_insert_new = f"""
    -- All updates (including brand-new customers) produce a new active row
    SELECT
    cu.customer_id,
    cu.customer_name,
    cu.city,
    cu.dob,
    cu.change_date AS effective_date,
    NULL AS expiry_date, 
    1 AS current_flag 
    FROM customer_updates cu
    -- no join to master -> insert rows for both existing and new customers
    """
    spark.sql(query_insert_new).createOrReplaceTempView("inserted_rows")


    # Step C: Combine all rows to form Final Table
    query_scd2_final = """
    SELECT * FROM invalidated_rows -- Old records, now expired
    UNION ALL
    SELECT * FROM inserted_rows    -- New records, now active
    UNION ALL
    -- Select all current records that were NOT affected by the update
    SELECT cm.* FROM customer_master_scd2 cm
    LEFT ANTI JOIN customer_updates cu ON cm.customer_id = cu.customer_id
    """
    df_scd2_final = spark.sql(query_scd2_final)
    df_scd2_final.createOrReplaceTempView("customer_master_scd2_final")

    print(f"\n--- Final State (Customer Master SCD II - Full Table Sample) ---")
    spark.sql("""
        SELECT customer_id, customer_name, city, effective_date, expiry_date, current_flag 
        FROM customer_master_scd2_final 
        ORDER BY customer_id, current_flag DESC -- Optional
    """).show(SHOW_ROWS, truncate=False)

    df_scd2_final.write.mode("overwrite").csv(f"{OUTPUT_PATH}/customer_master_scd2_final", header=True)
    print(f"Final SCD II table saved to {OUTPUT_PATH}/customer_master_scd2_final.csv")
    
    spark.stop()
    print("\nSpark Session Stopped. Job Complete.")

if __name__ == "__main__":
    run_spark_sql_job()