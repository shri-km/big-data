#!/usr/bin/env python3
# PySpark job for Dataproc. Works with libsvm formatted MNIST train file.
#
# Usage:
# gcloud dataproc jobs submit pyspark gs://BUCKET/code/train_tune_mnist.py --region=REGION --cluster=CLUSTER

import os
import json
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

BUCKET_NAME = os.environ.get("GCS_BUCKET", "your-gcs-bucket-name").replace("gs://", "").strip("/")
TRAIN_LIBSVM = os.environ.get("W9_TRAIN_LIBSVM", f"gs://{BUCKET_NAME}/w9/mnist")
MODEL_DIR = os.environ.get("W9_MODEL_DIR", "w9/model")
NUM_FOLDS = int(os.environ.get("NUM_FOLDS", "3"))
PARALLELISM = int(os.environ.get("PARALLELISM", "2"))

def write_json_to_gcs(bucket, path, payload):
        
        client = storage.Client()
        bucket = client.bucket(bucket)
        blob = bucket.blob(path)

        blob.upload_from_string(json.dumps(payload, indent=2), content_type="application/json")

        print(f"Wrote JSON to gs://{bucket}/{path}")

def main():

    spark = SparkSession.builder.appName("DT_MNIST_CV").getOrCreate()
    print("Spark session created")

    print(f"Loading train data from {TRAIN_LIBSVM}")
    df = spark.read.format("libsvm").load(TRAIN_LIBSVM)

    df = df.cache()
    print(f"Training rows: {df.count()} columns: {df.columns}")

    # DecisionTree expects columns: 'features' and 'label' (libsvm gives these)
    dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", seed=42)

    pipeline = Pipeline(stages=[dt])

    # Param grid - tuned hyperparameters (adjust or extend if desired)
    paramGrid = (ParamGridBuilder()
                 .addGrid(dt.maxDepth, [5, 10, 15])
                 .addGrid(dt.maxBins, [32, 64])
                 .addGrid(dt.minInstancesPerNode, [1, 2])
                 .build())

    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")

    cv = CrossValidator(estimator=pipeline,
                        estimatorParamMaps=paramGrid,
                        evaluator=evaluator,
                        numFolds=NUM_FOLDS,
                        parallelism=PARALLELISM)

    print("Starting CrossValidator.fit()")
    cvModel = cv.fit(df)
    print("Cross-validation finished")

    bestModel = cvModel.bestModel
    
    # bestModel is a PipelineModel. The last stage is the DecisionTreeModel.
    dt_stage = bestModel.stages[-1]

    best_params = {
        "maxDepth": int(dt_stage.getOrDefault(dt_stage.maxDepth)),
        "maxBins": int(dt_stage.getOrDefault(dt_stage.maxBins)),
        "minInstancesPerNode": int(dt_stage.getOrDefault(dt_stage.minInstancesPerNode)),
        "numFolds": NUM_FOLDS
    }

    print("Best hyperparameters found:", json.dumps(best_params))

    model_path = f"gs://{BUCKET_NAME}/{MODEL_DIR}"
    print(f"Saving model to {model_path}")
    bestModel.write().overwrite().save(model_path)
    print("Model saved")

    params_path = f"{MODEL_DIR}/best_params.json"
    write_json_to_gcs(BUCKET_NAME, params_path, best_params)

    spark.stop()
    print("Done")

if __name__ == "__main__":
    main()
