from pyspark.sql import SparkSession
from pyspark.sql.functions import col, stddev, max, sqrt, mean
from kafka import KafkaConsumer, KafkaProducer
import os, json, time, uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from collections import defaultdict, deque

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from minio import Minio

FAST_API_PORT = int(os.environ.get("FAST_API_MODEL_PREDICT_PORT", 8004))

# MinIO Connection
MINIO_ADDRESS = os.environ["MINIO_ADDRESS"]
MINIO_PORT = os.environ["MINIO_PORT"]
MINIO_USER = os.environ["MINIO_USER"]
MINIO_PASSWORD = os.environ["MINIO_PASSWORD"]

# Kafka Connection
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC_INPUT = os.environ["KAFKA_TOPIC_STREAMING"]
KAFKA_TOPIC_OUTPUT = os.environ["KAFKA_TOPIC_PREDICTION"]

# Load Model from MinIO
def load_model_from_minio():
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
    
    try:
        # Download metadata
        minio_client.fget_object(minio_bucket, metadata_path, local_metadata_path)
        print(f"---- Downloaded metadata from {metadata_path} to {local_metadata_path}")

        with open(local_metadata_path, "r") as f:
            metadata = json.load(f)
        print(f"Loaded metadata from {local_metadata_path} successfully")

        model = RandomForestClassificationModel.load(minio_url)
        print(f"Loaded model from {minio_url} successfully")

        print(f"Model metadata: {metadata[f'feature_columns']}")
        
        return model, metadata
        
    except Exception as e:
        print(f"Error loading model: {str(e)}")
        raise

# Predict New Data
def predict_new_data(model, metadata, new_data_df):
    # new_data_df.printSchema()

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
def cleanup_stale_records(producer, model, metadata, spark):
    current_time = time.time()
    # print(f"Checking for stale records at {current_time}...")
    expired_bookings = [bid for bid, t in booking_timestamps.items() if current_time - t > TIMEOUT_SECONDS]
    for bid in expired_bookings:
        if len(data_buffer[bid]) > 0:
            process_and_predict(bid, list(data_buffer[bid]), producer, model, metadata, spark)
            del data_buffer[bid]
            del booking_timestamps[bid]
            # print(f"Processed stale bookingID: {bid}")

# Process records and make predictions
def process_and_predict(bookingID, records, producer, model, metadata, spark):
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

    prediction = predict_new_data(model, metadata, processed_df.drop("bookingid"))
    prediction_message = {
        "bookingid": bookingID,
        "time": processed_df.select("second").first()[0],
        "label": int(prediction)
    }
    producer.send(KAFKA_TOPIC_OUTPUT, prediction_message)
    print(f"Sent prediction: {prediction_message}")

# Kafka Consumer
def consume_messages(consumer, producer, model, metadata, spark):
    try:
        for message in consumer:
            data = message.value
            bookingID = data["bookingid"]
            booking_timestamps[bookingID] = time.time()

            data_buffer[bookingID].append(data)

            # print(f"Received data for bookingid: {bookingID}, buffer size: {len(data_buffer[bookingID])}")

            # Trigger and slide window
            if len(data_buffer[bookingID]) == WINDOW_SIZE:
                # Get the records needed for prediction
                needed_records = list(data_buffer[bookingID])[:WINDOW_SIZE]

                # Slide window by removing STEP_SIZE oldest records
                data_buffer[bookingID] = deque(list(data_buffer[bookingID])[STEP_SIZE:])

                # print(f"Processing bookingid: {bookingID}, but now records count from {WINDOW_SIZE} to {len(data_buffer[bookingID])}")

                process_and_predict(bookingID, needed_records, producer, model, metadata, spark)
                
            cleanup_stale_records(producer, model, metadata, spark)  # Check for stale records
                
    except KeyboardInterrupt:
        print("Stopped consuming messages")
    finally:
        consumer.close()
        producer.close()

def start_streaming(modelid):
    
    print(f"==================== START STREAMING FOR MODEL {modelid} ====================")
    
    print("Listening for streaming data...")
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
    print("Connected to Kafka successfully")

    spark = SparkSession.builder \
                        .appName("PySpark Streaming Processor") \
                        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ADDRESS}:{MINIO_PORT}")\
                        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
                        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
                        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                        .getOrCreate()
    model, metadata = load_model_from_minio()
        
    consume_messages(consumer, producer, model, metadata, spark)
    returnString = "Streaming process started"
    
    print(f"===================={returnString} ====================")
    spark.stop()

    return returnString

app = FastAPI()

class Item(BaseModel):
    modelid: str

@app.post("/streaming")
async def predict(data: Item):
    result = start_streaming(data.modelid)
    return {"status": "success", "message": f"{result}"}

# Main function
if __name__ == "__main__":
    uvicorn.run("streaming_data_handling:app", host="0.0.0.0", port=FAST_API_PORT, reload=True)    
    # result = start_streaming()
    # print(f"Streaming process finished: {result}")
