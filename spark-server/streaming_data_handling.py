import asyncio
import os, json, time, uvicorn

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, stddev, max, sqrt, mean
from kafka import KafkaConsumer, KafkaProducer
from fastapi import FastAPI
from pydantic import BaseModel
from collections import defaultdict, deque
from minio import Minio

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

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

## Common spark object
def generateSparkSession():
    active_spark = SparkSession.getActiveSession()
    print(f"################## current spark session: {active_spark}")

    if active_spark:
        return active_spark
    try:
        return SparkSession.builder \
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
    except Exception as e:
        print(f"Error creating Spark session: ,WHYYYYYYYYYYYYYYYYYYYYYYYYYYY")
        return None

## Common model,metadata object
global_model = None
global_metadata = None
def set_model(model, metadata):
    if  model is None:
        return
    global global_model
    global global_metadata
    print(f"######## SETTING MODEL: {model}, {metadata}")
    global_model = model
    global_metadata = metadata
def get_model():
    global global_model
    global global_metadata
    print(f"######## GETTING MODEL: {global_model}, {global_metadata}")
    return global_model, global_metadata
# Load Model from MinIO
def load_model_from_minio():

    try:
        print(f"load model")

        minio_client = Minio(
            f"{MINIO_ADDRESS}:{MINIO_PORT}",
            access_key=MINIO_USER,
            secret_key=MINIO_PASSWORD,
            secure=False
        )

        minio_bucket = "models"
        minio_url = "s3a://models/sparkml_model_v1"  # Adjust if needed
        metadata_path = "rf_model/metadata_v1.json"

        local_metadata_path = "/tmp/model_metadata_v1.json"

        # Download metadata
        minio_client.fget_object(minio_bucket, metadata_path, local_metadata_path)
        print(f"---- Downloaded metadata from {metadata_path} to {local_metadata_path}")

        with open(local_metadata_path, "r") as f:
            metadata = json.load(f)
        print(f"Loaded metadata from {local_metadata_path} successfully")
        # Ensure SparkSession is initialized
        spark = generateSparkSession()
        print(f"GENERATED SPARK SESSION: {spark}")

        model = RandomForestClassificationModel.load(minio_url)
        print(f"Loaded model from {minio_url} successfully")

        print(f"Model metadata: {metadata[f'feature_columns']}")
        set_model(model, metadata)
        print(f"load model {model}, {metadata}")
        return model, metadata

    except Exception as e:
        print(f"Error loading model: {str(e)}")
        return None, None
# Predict New Data
def predict_new_data(new_data_df):
    # new_data_df.printSchema()
    model, metadata = get_model()
    print(f"predict_new_data 1: {model}, {metadata}")
    if model is None:
        model, metadata = load_model_from_minio()

    if model is None:
        return None

    feature_columns = [col for col in new_data_df.columns if col != "label"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    assembled_data = assembler.transform(new_data_df)

    predict_result = model.transform(assembled_data)

    # print("Predict result schema:")
    # predict_result.printSchema()
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

    print("predict 1")
    spark = generateSparkSession()

    df = spark.createDataFrame(records)

    print("predict 2")
    # Calculate instantaneous magnitude features
    df = df.withColumn("accel_mag", sqrt(col("acceleration_x")**2 +
                                        col("acceleration_y")**2 +
                                        col("acceleration_z")**2)) \
           .withColumn("gyro_mag", sqrt(col("gyro_x")**2 +
                                      col("gyro_y")**2 +
                                      col("gyro_z")**2))

    print("predict 3")
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

    print("predict 4")
    prediction = predict_new_data(processed_df.drop("bookingid"))
    print("predict 5")
    prediction_message = {
        "bookingid": bookingID,
        "time": processed_df.select("second").first()[0],
        "speed": processed_df.select("avg_speed").first()[0],
        "label": int(prediction)
    }
    print("predict 6")
    producer.send(KAFKA_TOPIC_OUTPUT, prediction_message)
    print(f"Sent prediction: {prediction_message}")

# Kafka Consumer
def consume_messages():
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

def refresh_model(modelid):

    print(f"==================== refresh model: {modelid} ====================")

    model, metadata = load_model_from_minio()
    returnString = f"refresh model: {modelid} - Completed"

    print(f"==================== {returnString} ====================")

    return returnString

app = FastAPI()

class Item(BaseModel):
    modelid: str

@app.post("/refresh_model")
async def refresh_model_call(data: Item):
    result = refresh_model(data.modelid)
    return {"status": "success", "message": f"{result}"}

@app.get("/")
async def root():
    return {"message": "FastAPI server is running"}
# Main function
# if __name__ == "__main__":
#     uvicorn.run("streaming_data_handling:app", host="0.0.0.0", port=FAST_API_PORT, reload=True)
    # result = start_streaming()
    # print(f"Streaming process finished: {result}")
# Main function to run both entry points
# Start FastAPI server
async def start_fastapi():
    print(f"starting fast api server on port {FAST_API_PORT}...")
    uvicorn.run("streaming_data_handling:app", host="0.0.0.0", port=FAST_API_PORT, reload=True)
    # await asyncio.to_thread(uvicorn.run, "streaming_data_handling:app", host="0.0.0.0", port=8005, reload=True)

    print(f"starting fast api server on port {FAST_API_PORT}...Completed")
    # try:
    #
    #     print(f"Starting FastAPI server on port {FAST_API_PORT}...")
    #     config = uvicorn.Config("streaming_data_handling:app", host="0.0.0.0", port=FAST_API_PORT, reload=False)
    #     server = uvicorn.Server(config)
    #     print(f"Starting FastAPI server on port {FAST_API_PORT}... - start up success")
    #     server.serve()
    # except Exception as e:
    #     print(f"Error starting FastAPI server: {e}")


async def start_kafka_consumer_task():
    print(f"starting Kafka consumer...")
    # consume_messages(model,metadata)
    await asyncio.to_thread(consume_messages)

    print(f"starting Kafka consumer... Completed ")

async def main():
    # Run Kafka consumer and FastAPI server concurrently

    await asyncio.gather(
        start_kafka_consumer_task(),
        start_fastapi()

        # asyncio.to_thread(start_fastapi),
    )
    print("hehe")
    # uvicorn.run("streaming_data_handling:app", host="0.0.0.0", port=FAST_API_PORT, reload=True)

    print("hehe the end")


if __name__ == "__main__":
    asyncio.run(main())

import socket


# def is_port_in_use(port):
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         return s.connect_ex(("localhost", port)) == 0
#
#
# print(f"is port in use 8004: {is_port_in_use(8003)}")
# print(f"is port in use 8005: {is_port_in_use(8004)}")
# print(f"is port in use 8006: {is_port_in_use(8005)}")