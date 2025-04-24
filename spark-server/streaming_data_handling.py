import asyncio
import os, json, time, uvicorn
import threading

from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, stddev, max, sqrt, mean
from kafka import KafkaConsumer, KafkaProducer
from fastapi import FastAPI
from pydantic import BaseModel
from collections import defaultdict, deque

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

FAST_API_PORT = int(os.environ.get("FAST_API_MODEL_PREDICT_PORT", 8005))

# MinIO Connection
MINIO_ADDRESS = os.environ["MINIO_ADDRESS"]
MINIO_PORT = os.environ["MINIO_PORT"]
MINIO_USER = os.environ["MINIO_USER"]
MINIO_PASSWORD = os.environ["MINIO_PASSWORD"]

# Kafka Connection
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC_INPUT = os.environ["KAFKA_TOPIC_STREAMING"]
KAFKA_TOPIC_OUTPUT = os.environ["KAFKA_TOPIC_PREDICTION"]

# MLflow config
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
ALIAS_NAME = "Champion"

# MLflow config
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
ALIAS_NAME = "champion"
MODEL_NAME = "RandomForest_Telematic"

##### METHOD 1: with shared variable
# shared_variable = {"model": None,"metadata":None}
# shared_lock = threading.Lock()
#
# def get_model():
#     global shared_variable
#     with shared_lock:
#         return shared_variable["model"], shared_variable["metadata"]
# def set_model(model, metadata):
#     global shared_variable
#     with shared_lock:
#         shared_variable["model"] = model
#         shared_variable["metadata"] = metadata


##### METHOD 2: with static method
class SharedModel:
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self.model = None
        self.metadata = None

    @staticmethod
    def get_instance():
        if SharedModel._instance is None:
            with SharedModel._lock:
                if SharedModel._instance is None:
                    SharedModel._instance = SharedModel()
        return SharedModel._instance

    def get_model(self):
        with SharedModel._lock:
            return self.model, self.metadata

    def set_model(self, model, metadata):
        with SharedModel._lock:
            self.model = model
            self.metadata = metadata

def get_model():
    return SharedModel.get_instance().get_model()

def set_model(model, metadata):
    SharedModel.get_instance().set_model(model, metadata)

shared_model = SharedModel.get_instance()  # Initialize the singleton instance

class SparkSessionSingleton:
    _instance = None
    _lock = threading.Lock()  # Lock to ensure thread-safe access
    _lock2 = threading.Lock()  # Lock to ensure thread-safe access

    @staticmethod
    def get_instance():
        with SparkSessionSingleton._lock:  # Ensure only one thread can access this block at a time
            if SparkSessionSingleton._instance is None:
                SparkSessionSingleton._instance = SparkSession.builder \
                .appName("PySpark Streaming Processor") \
                .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ADDRESS}:{MINIO_PORT}") \
                .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
                .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.dynamicAllocation.enabled", "false") \
                .getOrCreate()
        return SparkSessionSingleton._instance
    #

    ##### METHOD 3: trying to add model to spark session singleton
    # _model = None
    # _metadata = None
    #
    # @staticmethod
    # def get_model():
    #     with SparkSessionSingleton._lock2:  # Ensure only one thread can access this block at a time
    #
    #         print(f"GETTING MODEL: { SparkSessionSingleton._model}, {SparkSessionSingleton._metadata}")
    #         return SparkSessionSingleton._model,SparkSessionSingleton._metadata
    #
    # @staticmethod
    # def set_model(model, metadata):
    #     print(f"SETTING MODEL WAS: { SparkSessionSingleton._model}, {SparkSessionSingleton._metadata}")
    #     if model is None:
    #         return
    #     with SparkSessionSingleton._lock2:  # Ensure only one thread can access this block at a time
    #
    #         SparkSessionSingleton._model = model
    #         SparkSessionSingleton._metadata = metadata
    #         print(f"SETTING MODEL NOW: { SparkSessionSingleton._model}, {SparkSessionSingleton._metadata}")

def generateSparkSession():
   return SparkSessionSingleton.get_instance()

# Load Model from MinIO
def load_model_from_mlflow(modelname):
    client = MlflowClient()

    # === Step 1: Try to get model version by alias ===
    try:
        model_version = client.get_model_version_by_alias(modelname, ALIAS_NAME)
        run_id = model_version.run_id
        version = model_version.version
        print(f"[INFO] Found alias '{ALIAS_NAME}' for model '{modelname}': version={version}, run_id={run_id}")
    except Exception as e:
        # Fallback: Get the latest version instead
        print(f"[WARN] Alias '{ALIAS_NAME}' not found. Falling back to latest registered version.")
        versions = client.search_model_versions(f"name='{modelname}'")
        if not versions:
            raise RuntimeError(f"No versions found for model '{modelname}'.")
        latest_version_info = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
        version = latest_version_info.version
        run_id = latest_version_info.run_id
        print(f"[INFO] Fallback to version={version}, run_id={run_id}")

    # === Step 2: Load the model ===
    model_uri = f"models:/{modelname}/{version}"
    model = mlflow.spark.load_model(model_uri)
    print(f"[INFO] Model loaded from URI: {model_uri}")

    # Step 3: Download and load metadata
    local_dir = "/tmp/mlflow_artifacts"
    os.makedirs(local_dir, exist_ok=True)

    metadata_path = client.download_artifacts(run_id, "model_metadata.json", local_dir)
    with open(metadata_path, "r") as f:
        metadata = json.load(f)

    print("[INFO] Loaded model metadata:")
    print(json.dumps(metadata, indent=2))

    return model, metadata

# Predict New Data
def predict_new_data(new_data_df):
    # new_data_df.printSchema()
    model, metadata = get_model()
    if model is None:
        model, metadata = load_model_from_mlflow(MODEL_NAME)
        set_model(model, metadata)

    if model is None:
        return None

    # feature_columns = [col for col in new_data_df.columns if col != "label"]
    feature_columns = metadata.get("feature_columns")
    if not feature_columns:
        raise ValueError("Feature columns missing in model metadata.")

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    assembled_data = assembler.transform(new_data_df)

    predict_result = model.transform(assembled_data)

    return predict_result.select("prediction").first()[0]


# In-memory buffer for tracking records per bookingID
data_buffer = defaultdict(deque)  # Stores up to 15 records per bookingID
booking_timestamps = {}  # Tracks last received time per bookingID
TIMEOUT_SECONDS = 30
WINDOW_SIZE = 10 # Number of records to trigger prediction
STEP_SIZE = 5 # Number of records to step forward

# Background thread to process stale records
def cleanup_stale_records(producer):
    current_time = time.time()
    # print(f"Checking for stale records at {current_time}...")
    expired_bookings = [bid for bid, t in booking_timestamps.items() if current_time - t > TIMEOUT_SECONDS]
    for bid in expired_bookings:
        if len(data_buffer[bid]) > 0:
            process_and_predict(bid, list(data_buffer[bid]), producer)
            del data_buffer[bid]
            del booking_timestamps[bid]
            # print(f"Processed stale bookingID: {bid}")

# Process records and make predictions
def process_and_predict(bookingID, records, producer):

    spark = generateSparkSession()

    df = spark.createDataFrame(records)

    # Calculate instantaneous magnitude features
    df = df.withColumn("accel_mag", sqrt(col("acceleration_x")**2 +
                                        col("acceleration_y")**2 +
                                        col("acceleration_z")**2)) \
           .withColumn("gyro_mag", sqrt(col("gyro_x")**2 + 
                                      col("gyro_y")**2 + 
                                      col("gyro_z")**2))
    
    processed_df = df.groupBy("bookingid").agg(
        mean("speed").alias("avg_speed"),
        stddev("speed").alias("std_speed"),
        
        mean("accel_mag").alias("avg_accel_mag"),
        max("accel_mag").alias("max_accel_mag"),
        stddev("accel_mag").alias("std_accel_mag"),
        
        mean("gyro_mag").alias("avg_gyro_mag"),
        stddev("gyro_mag").alias("std_gyro_mag"),
        
        mean("acceleration_x").alias("avg_accel_x"),
        stddev("acceleration_x").alias("std_accel_x"),
        max("acceleration_x").alias("max_accel_x"),
        
        mean("acceleration_y").alias("avg_accel_y"),
        stddev("acceleration_y").alias("std_accel_y"),
        max("acceleration_y").alias("max_accel_y"),
        
        mean("acceleration_z").alias("avg_accel_z"),
        stddev("acceleration_z").alias("std_accel_z"),
        max("acceleration_z").alias("max_accel_z"),
        
        mean("gyro_x").alias("avg_gyro_x"),
        stddev("gyro_x").alias("std_gyro_x"),
        
        mean("gyro_y").alias("avg_gyro_y"),
        stddev("gyro_y").alias("std_gyro_y"),
        
        mean("gyro_z").alias("avg_gyro_z"),
        stddev("gyro_z").alias("std_gyro_z"),
        
        mean("accuracy").alias("avg_accuracy"),
        stddev("accuracy").alias("std_accuracy"),
        
        max("second").alias("second"),
    )

    prediction = predict_new_data( processed_df.drop("bookingid"))
    prediction_message = {
        "bookingid": bookingID,
        "time": processed_df.select("second").first()[0],
        "speed": processed_df.select("avg_speed").first()[0],
        "label": int(prediction)
    }
    producer.send(KAFKA_TOPIC_OUTPUT, prediction_message)
    print(f"Sent prediction: {prediction_message}")

# Kafka Consumer
def consume_messages():
    # CHECKER FOR MODEL print(f"@@@@@@@@@@@@@@@@@@@@@@@@ global_item in consume_messages: {get_model()}")

    spark = None
    consumer = None
    producer = None
    try:
        spark = generateSparkSession()
        consumer = KafkaConsumer(
            KAFKA_TOPIC_INPUT,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        for message in consumer:
            global shared_model
            # CHECKER FOR MODEL print(f"@@@@@@@@@@@@@@@@@@@@@@@@ global_item in consume_messages loop: {get_model()}")

            data = message.value
            bookingID = data["bookingid"]
            booking_timestamps[bookingID] = time.time()

            data_buffer[bookingID].append(data)

            print(f"Received data for bookingid: {bookingID}, buffer size: {len(data_buffer[bookingID])}")

            # Trigger and slide window
            if len(data_buffer[bookingID]) == WINDOW_SIZE:
                # Get the records needed for prediction
                needed_records = list(data_buffer[bookingID])[:WINDOW_SIZE]

                # Slide window by removing STEP_SIZE oldest records
                data_buffer[bookingID] = deque(list(data_buffer[bookingID])[STEP_SIZE:])

                # print(f"Processing bookingid: {bookingID}, but now records count from {WINDOW_SIZE} to {len(data_buffer[bookingID])}")

                process_and_predict(bookingID, needed_records, producer)

            cleanup_stale_records(producer)  # Check for stale records

    except KeyboardInterrupt:
        print("Stopped consuming messages")
    finally:
        if consumer is not None:
            consumer.close()
        if producer is not None:
            producer.close()

def refresh_model(modelname):
    global MODEL_NAME
    MODEL_NAME = modelname

    print(f"==================== refresh model: {MODEL_NAME} ====================")
    global shared_model

    # CHECKER FOR MODEL print(f"@@@@@@@@@@@@@@@@@@@@@@@@ global_item in fast api call: {get_model()}")
    model, metadata = load_model_from_mlflow(MODEL_NAME)

    set_model(model, metadata)
    returnString = f"refresh model: {MODEL_NAME} - Completed"

    print(f"==================== {returnString} ====================")

    return returnString

app = FastAPI()

class post_item(BaseModel):
    modelname: str

@app.post("/refresh_model")
async def refresh_model_call(data: post_item):
    result = refresh_model(data.modelname)
    return {"status": "success", "message": f"{result}"}

thread = None
## HACKY way to start kafka using the thread from fast api, so that both thread can share the variable
@app.get("/")
async def root():
    global thread
    if thread is None:
        thread = threading.Thread(target=start_kafka_consumer_task, args=(), daemon=True)
        thread.start()
    # kafka_thread = threading.Thread(target=start_kafka_consumer_task,args=(), daemon=True)
    # kafka_thread.start()
    # kafka_thread.join()
    return {"message": "FastAPI server is running"}

def start_fastapi():
    print(f"starting fast api server on port {FAST_API_PORT}...")
    uvicorn.run("streaming_data_handling:app", host="0.0.0.0", port=FAST_API_PORT, reload=False)

    print(f"starting fast api server on port {FAST_API_PORT}...COMPLETED")
import threading

def start_kafka_consumer_task():
    print(f"starting Kafka consumer...")
    consume_messages()

def main():

    fastapi_thread = threading.Thread(target=start_fastapi,args=(), daemon=True)
    # kafka_thread = threading.Thread(target=start_kafka_consumer_task,args=(), daemon=True)

    fastapi_thread.start()
    # kafka_thread.start()

    fastapi_thread.join()
    # kafka_thread.join()


if __name__ == "__main__":
    main()

