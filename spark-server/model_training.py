import json

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import os, uvicorn
from fastapi import FastAPI

from minio import Minio

FAST_API_PORT = int(os.environ.get("FAST_API_MODEL_TRAINING_PORT", 8004))

# MinIO Connection
MINIO_ADDRESS = os.environ["MINIO_ADDRESS"]
MINIO_PORT = os.environ["MINIO_PORT"]
MINIO_USER = os.environ["MINIO_USER"]
MINIO_PASSWORD = os.environ["MINIO_PASSWORD"]

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

def upload_model(model):
    
    print("====================START OF UPLOADING====================")
    
    # Upload the model to MinIO
    minio_client = Minio(
        f"{MINIO_ADDRESS}:{MINIO_PORT}",
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False    # set to True if using HTTPS
    )

    # Define bucket and model path
    bucket_name = "models"
    model_path = "/tmp/random-forest-model/v1"
    model_file_name = "sparkml_model_v1"

    # Save the Spark ML model locally
    model.write().overwrite().save(model_path)

    # Ensure the bucket exists
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")

    # Upload all model files to MinIO
    for root, _, files in os.walk(model_path):
        for file in files:
            file_path = os.path.join(root, file)
            object_name = os.path.relpath(file_path, model_path).replace("\\", "/")  # Fix path for Windows/Linux
            minio_client.fput_object(bucket_name, f"{model_file_name}/{object_name}", file_path) # Upload model files
            print(f"Uploaded: {file_path} -> {model_file_name}/{object_name}")
            minio_client.fput_object(
                bucket_name,
                "rf_model/metadata_v1.json",
                "/tmp/model_metadata_v1.json"
            ) # Upload metadata
            print(f"Uploaded: /tmp/model_metadata_v1.json -> rf_model/metadata_v1.json")

    print("Model uploaded to MinIO successfully.")

def train_model():
    
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
        "std_accel_y", "std_accel_z", "std_gyro_x", "avg_accel_z", "avg_accel_y"
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
        "seed": 42
    }

    with open("/tmp/model_metadata_v1.json", "w") as f:
        json.dump(model_info, f)

    # Define Random Forest model
    rf = RandomForestClassifier(
        labelCol="label", 
        featuresCol="features", 
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    rf_model = rf.fit(train_data)

    # Evaluate the model
    predictions = rf_model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(f"Model Accuracy: {accuracy}")
    
    upload_model(rf_model)
    
    print("==================== TRAINING & UPLOADING COMPLETED ====================")
    spark.stop()
    
    returnStr = f"Model trained and uploaded successfully with accuracy: {accuracy}"
    return returnStr


app = FastAPI()

@app.post("/train")
async def predict():
    result = train_model()

    return {"status": "success", "message": f"{result}"}

if __name__ == "__main__":
    uvicorn.run("model_training:app", host="0.0.0.0", port=FAST_API_PORT, reload=True)