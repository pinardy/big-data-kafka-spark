import json

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import os, uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

import mlflow
import mlflow.spark

FAST_API_PORT = int(os.environ.get("FAST_API_MODEL_TRAINING_PORT", 8004))

# PostgreSQL Connection
POSTGRES_ADDRESS = os.environ["POSTGRES_ADDRESS"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# Database connection properties
jdbc_url = f"jdbc:postgresql://{POSTGRES_ADDRESS}:{POSTGRES_PORT}/postgres"
db_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# MLflow config
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("random_forest_training")

MODEL_NAME = "RandomForest_Telematic"

def train_model(modelname):
    
    print("====================START OF TRAINING====================")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("RandomForestModelTraining") \
        .getOrCreate()

    # Load data from PostgreSQL table
    df = spark.read.jdbc(url=jdbc_url, table="telematics", properties=db_properties)
    print(f"Load data from PostgreSQL table: {df.count()} rows loaded.")

    # Preprocessing: Assemble features into a single vector
    feature_columns = [
        "avg_gyro_mag", "avg_speed", "std_gyro_z", "max_accel_z",
        "std_accel_y", "std_accel_z", "std_gyro_x", "avg_accel_z", "avg_accel_y", "second"
    ]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(df)

    # Split data into training and testing sets
    train_data, test_data = df.randomSplit([0.7, 0.3], seed=42)

    model_info = {
        "feature_columns": feature_columns,
        "model_type": "RandomForestClassifier",
        "num_trees": 50,
        "max_depth": 10,
        "seed": 42,
        "training_data_count": train_data.count(),
        "testing_data_count": test_data.count(),
        "model_name": modelname
    }

    with mlflow.start_run():
        mlflow.log_params(model_info)

        rf = RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        rf_model = rf.fit(train_data)

        predictions = rf_model.transform(test_data)
        evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
        auc = evaluator.evaluate(predictions)

        mlflow.log_metric("AUC", auc)

        # Save metadata file to disk and log as artifact
        metadata_path = "/tmp/model_metadata_v1.json"
        with open(metadata_path, "w") as f:
            json.dump(model_info, f)
        mlflow.log_artifact(metadata_path)

        # Log the model to MLflow
        mlflow.spark.log_model(
            spark_model=rf_model, 
            artifact_path="spark-rf-model",
            registered_model_name=modelname,  # Register the model with a name
        )

        print(f"Model AUC: {auc}")
        print("==================== TRAINING & LOGGING COMPLETED ====================")
        spark.stop()

    return f"Model trained and logged to MLflow with AUC: {auc}"


app = FastAPI()

class ModelRequest(BaseModel):
    modelname: str

@app.post("/train")
async def train(req: ModelRequest):
    result = train_model(req.modelname)

    return {"status": "success", "message": f"{result}"}

if __name__ == "__main__":
    uvicorn.run("model_training:app", host="0.0.0.0", port=FAST_API_PORT, reload=True)