from pyspark.sql import SparkSession
from pyspark.sql.functions import col, stddev, min, max
from kafka import KafkaConsumer, KafkaProducer
import os, json, time
from collections import defaultdict, deque

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from minio import Minio

# MinIO Connection
MINIO_ADDRESS = os.environ["MINIO_ADDRESS"]
MINIO_PORT = os.environ["MINIO_PORT"]
MINIO_USER = os.environ["MINIO_USER"]
MINIO_PASSWORD = os.environ["MINIO_PASSWORD"]

# Kafka Connection
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC_INPUT = os.environ["KAFKA_TOPIC_INPUT"]
KAFKA_TOPIC_OUTPUT = os.environ["KAFKA_TOPIC_OUTPUT"]

# Download folder from MinIO - [stupid function]
def download_folder(minio_client, bucket_name, folder_path, local_dir):
    objects = minio_client.list_objects(bucket_name, prefix=folder_path, recursive=True)
    for obj in objects:
        # Ensure local directory structure exists
        local_path = os.path.join(local_dir, obj.object_name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        # Download file
        minio_client.fget_object(bucket_name, obj.object_name, local_path)
    print(f"Downloaded all files from {folder_path} to {local_dir}")

# Load Model from MinIO
def load_model_from_minio():
    minio_client = Minio(
        f"{MINIO_ADDRESS}:{MINIO_PORT}",
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )
    
    bucket_name = "models"
    model_folder = "sparkml_model/"
    metadata_path = "rf_model/metadata.json"
    
    local_model_dir = "/tmp/sparkml_model"
    local_metadata_path = "/tmp/model_metadata.json"
    
    try:
        # 1. Download entire model folder
        download_folder(minio_client, bucket_name, model_folder, local_model_dir)
        print(f"Downloaded model folder from {model_folder} to {local_model_dir}")
        
        # 2. Download metadata
        minio_client.fget_object(bucket_name, metadata_path, local_metadata_path)
        print(f"Downloaded metadata from {metadata_path} to {local_metadata_path}")
        
        # Load model and metadata
        model = RandomForestClassificationModel.load(local_model_dir)
        print(f"Loaded model from {local_model_dir} successfully")
        
        with open(local_metadata_path, "r") as f:
            metadata = json.load(f)
        print(f"Loaded metadata from {local_metadata_path} successfully")
        
        return model, metadata
        
    except Exception as e:
        print(f"Error loading model: {str(e)}")
        raise

# Predict New Data
def predict_new_data(model, metadata, new_data_df):
    assembler = VectorAssembler(
        inputCols=metadata["feature_columns"],
        outputCol="features"
    )
    assembled_data = assembler.transform(new_data_df)
    return model.transform(assembled_data)

# In-memory buffer for tracking records per bookingID
data_buffer = defaultdict(deque)  # Stores up to 15 records per bookingID
booking_timestamps = {}  # Tracks last received time per bookingID
TIMEOUT_SECONDS = 30
WINDOW_SIZE = 10 # Number of records to trigger prediction
STEP_SIZE = 5 # Number of records to step forward

# Background thread to process stale records
def cleanup_stale_records(producer, model, metadata, spark):
    while True:
        time.sleep(5)  # Check every 5 seconds
        current_time = time.time()
        print(f"Checking for stale records at {current_time}...")
        expired_bookings = [bid for bid, t in booking_timestamps.items() if current_time - t > TIMEOUT_SECONDS]
        for bid in expired_bookings:
            if len(data_buffer[bid]) > 0:
                process_and_predict(bid, list(data_buffer[bid]), producer, model, metadata, spark)
                del data_buffer[bid]
                del booking_timestamps[bid]
                print(f"Processed stale bookingID: {bid}")

# Process records and make predictions
def process_and_predict(bookingID, records, producer, model, metadata, spark):
    df = spark.createDataFrame(records)
    processed_df = df.groupBy("bookingID").agg(
        max("Speed").alias("Speed_max"),
        stddev("Speed").alias("Speed_std"),
        min("acceleration_x").alias("acceleration_x_min"),
        max("acceleration_z").alias("acceleration_z_max"),
        stddev("acceleration_x").alias("acceleration_x_std"),
        stddev("acceleration_y").alias("acceleration_y_std"),
        stddev("acceleration_z").alias("acceleration_z_std"),
        stddev("Bearing").alias("Bearing_std"),
        max("second").alias("time"),
        stddev("gyro_x").alias("gyro_x_std"),
        stddev("gyro_y").alias("gyro_y_std"),
        stddev("gyro_z").alias("gyro_z_std"),
    ).withColumn("Speed_perc70", col("Speed_max") * 0.7)
    
    prediction = predict_new_data(model, metadata, processed_df.drop("bookingID"))
    prediction_message = {
        "bookingID": bookingID,
        "time": processed_df.select("time").first()[0],
        "label": int(prediction)
    }
    producer.send(KAFKA_TOPIC_OUTPUT, prediction_message)
    print(f"✅ Sent prediction: {prediction_message}")

# Kafka Consumer
def consume_messages(consumer, producer, model, metadata, spark):
    try:
        for message in consumer:
            data = message.value
            bookingID = data["bookingID"]
            records = data["records"]
            booking_timestamps[bookingID] = time.time()
            
            for record in records:
                record["bookingID"] = bookingID
                data_buffer[bookingID].append(record)
                
                if len(data_buffer[bookingID]) > 15:
                    data_buffer[bookingID].popleft()  # Keep only last 15 records
                    
                    # Trigger and slide window
                    if len(data_buffer[bookingID]) == WINDOW_SIZE:
                        process_and_predict(bookingID, list(data_buffer[bookingID]), producer, model, metadata, spark)
                        
                        # Slide window by removing STEP_SIZE oldest records
                        data_buffer[bookingID] = deque(list(data_buffer[bookingID])[STEP_SIZE:])
                
            cleanup_stale_records(producer, model, metadata, spark)  # Check for stale records
                
    except KeyboardInterrupt:
        print("Stopped consuming messages")
    finally:
        consumer.close()
        producer.close()

# Main function
if __name__ == "__main__":
    print("Listening for streaming data...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC_INPUT,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    spark = SparkSession.builder.appName("PySpark Streaming Processor").getOrCreate()
    model, metadata = load_model_from_minio()
        
    consume_messages(consumer, producer, model, metadata, spark)
