import os
from pyspark.sql import SparkSession

_bucket = os.environ.get("GCS_BUCKET", "your-gcs-bucket-name").replace("gs://", "").strip("/")
FULL_TEST_LIBSVM = os.environ.get("W9_FULL_TEST_LIBSVM", f"gs://{_bucket}/w9/mnist.t")
SAMPLED_TEST_OUT = os.environ.get("W9_SAMPLED_TEST_OUT", f"gs://{_bucket}/w9/mnist_test_sampled")

NUM_SAMPLES = int(os.environ.get("NUM_SAMPLES", "10"))
SEED = 42

def main():
    spark = SparkSession.builder.appName("Sample_MNIST_Test").getOrCreate()

    print(f"Reading full MNIST test set from {FULL_TEST_LIBSVM}")
    df = spark.read.format("libsvm").load(FULL_TEST_LIBSVM)

    print(f"Total test rows: {df.count()}")

    sampled_df = df.orderBy("label").sample(False, NUM_SAMPLES / df.count(), seed=SEED).limit(NUM_SAMPLES)

    print(f"Writing {NUM_SAMPLES} sampled rows to {SAMPLED_TEST_OUT}")
    sampled_df.coalesce(1).write.mode("overwrite").format("libsvm").save(SAMPLED_TEST_OUT)

    sampled_df.show(10, truncate=False)

    spark.stop()
    print("Sampling complete")

if __name__ == "__main__":
    main()
