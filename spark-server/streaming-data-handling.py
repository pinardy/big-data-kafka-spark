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
KAFKA_TOPIC_INPUT = os.environ["KAFKA_TOPIC_STREAM"]
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
    minio_url = "s3a://models/sparkml_model"  # Adjust if needed
    metadata_path = "rf_model/metadata.json"

    local_metadata_path = "/tmp/model_metadata.json"
    
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
    new_data_df.printSchema()

    feature_columns = [col for col in new_data_df.columns if col != "label"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    assembled_data = assembler.transform(new_data_df)

    predict_result = model.transform(assembled_data)

    print("Predict result schema:")
    predict_result.printSchema()
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
    print(f"Sent prediction: {prediction_message}")

# Kafka Consumer
def consume_messages(consumer, producer, model, metadata, spark):
    try:
        for message in consumer:
            data = message.value
            bookingID = data["bookingID"]
            booking_timestamps[bookingID] = time.time()

            data_buffer[bookingID].append(data)

            print(f"Received data for bookingID: {bookingID}, buffer size: {len(data_buffer[bookingID])}")

            # Trigger and slide window
            if len(data_buffer[bookingID]) == WINDOW_SIZE:
                # Get the records needed for prediction
                needed_records = list(data_buffer[bookingID])[:WINDOW_SIZE]

                # Slide window by removing STEP_SIZE oldest records
                data_buffer[bookingID] = deque(list(data_buffer[bookingID])[STEP_SIZE:])

                print(f"Processing bookingID: {bookingID}, but now records count from {WINDOW_SIZE} to - {len(data_buffer[bookingID])}")

                process_and_predict(bookingID, needed_records, producer, model, metadata, spark)
                
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
