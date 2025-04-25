import json, os, uvicorn

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

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

def train_model(modelname: str, feature_columns: list[str]):
    print(f"====================START OF TRAINING MODEL {modelname}====================")
    
    spark = SparkSession.builder \
        .appName("RandomForestModelTraining") \
        .getOrCreate()

    try:
        # Load data from PostgreSQL table
        df = spark.read.jdbc(url=jdbc_url, table="telematics", properties=db_properties)
        print(f"Load data from PostgreSQL table: {df.count()} rows loaded.")

        # Verify all requested columns exist in the dataframe
        missing_cols = [col for col in feature_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns not found in data: {missing_cols}")

        # Preprocessing: Assemble features into a single vector
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        df = assembler.transform(df)

        # Split data
        train_data, test_data = df.randomSplit([0.7, 0.3], seed=42)

        model_info = {
            "feature_columns": feature_columns,
            "model_type": "RandomForestClassifier",
            "num_trees": 100,
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
            evaluator = BinaryClassificationEvaluator(
                labelCol="label",
                rawPredictionCol="rawPrediction",
                metricName="areaUnderROC"
            )
            auc = evaluator.evaluate(predictions)
            mlflow.log_metric("AUC", auc)

            # Save metadata
            metadata_path = "/tmp/model_metadata.json"
            with open(metadata_path, "w") as f:
                json.dump(model_info, f)
            mlflow.log_artifact(metadata_path)

            # Log the model
            mlflow.spark.log_model(
                spark_model=rf_model, 
                artifact_path="spark-rf-model",
                registered_model_name=modelname,
            )

            print(f"Model AUC: {auc}")
            print("==================== TRAINING COMPLETED ====================")
            
            return {"status": "success", "auc": auc, "features_used": feature_columns}
            
    except Exception as e:
        print(f"Training failed: {str(e)}")
        return {"status": "error", "message": str(e)}
    finally:
        spark.stop()

app = FastAPI()

class ModelRequest(BaseModel):
    modelname: str  # Model name from request
    feature_columns: list[str]  # Dynamic feature columns from request

@app.post("/train")
async def train(req: ModelRequest):
    result = train_model(req.modelname, req.feature_columns)

    return {"status": "success", "message": f"{result}"}

if __name__ == "__main__":
    uvicorn.run("model_training:app", host="0.0.0.0", port=FAST_API_PORT, reload=True)