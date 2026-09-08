import os
from sys import exit
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

_bucket = os.environ.get("GCS_BUCKET", "your-gcs-bucket-name").replace("gs://", "").strip("/")
MODEL_PATH = os.environ.get("W9_MODEL_PATH", f"gs://{_bucket}/w9/model/")
TEST_LIBSVM = None          # will be injected by GCF
OUTPUT_PATH = None         # will be injected by GCF

def main(test_path, output_path):

    if not (test_path and output_path):
        print("No test path and output path provided. Aborting.")
        exit()

    spark = SparkSession.builder.appName("MNIST_DT_Inference").getOrCreate()

    print(f"Loading model from {MODEL_PATH}")
    model = PipelineModel.load(MODEL_PATH)

    print(f"Reading test data from {test_path}")
    test_df = spark.read.format("libsvm").load(test_path)

    preds = model.transform(test_df)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )

    acc = evaluator.evaluate(preds)
    print(f"Inference Accuracy on sampled test set: {acc}")

    preds.select("label", "prediction", "probability").show(10, truncate=False)
    preds = preds.withColumn("probability_str", col("probability").cast("string"))

    print(f"Writing predictions to {output_path}")
    preds.select("label", "prediction", "probability_str") \
         .coalesce(1) \
         .write.mode("overwrite") \
         .option("header", "true") \
         .csv(output_path)

    spark.stop()

if __name__ == "__main__":
    import sys
    TEST_LIBSVM = sys.argv[1]
    OUTPUT_PATH = sys.argv[2]
    main(TEST_LIBSVM, OUTPUT_PATH)
