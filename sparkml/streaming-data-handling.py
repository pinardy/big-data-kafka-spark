from pyspark.sql import SparkSession
from pyspark.sql.functions import col, stddev, min, max
from kafka import KafkaConsumer, KafkaProducer
import os, json

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

# Load Model from MinIO - not finished
def load_model_from_minio():
    # Initialize MinIO client
    minio_client = Minio(
        f"{MINIO_ADDRESS}:{MINIO_PORT}",
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=False
    )

    # Define the bucket and model path
    bucket_name = "models"
    model_path = "sparkml_model" 
    metadata_path = "rf_model/metadata.json"

    # Download the model file from MinIO
    local_model_path = "/tmp/sparkml_model"
    local_metadata_path = "/tmp/model_metadata.json"
    minio_client.fget_object(bucket_name, model_path, local_model_path)
    minio_client.fget_object(bucket_name, metadata_path, local_metadata_path)

    # Load the Spark ML model
    model = RandomForestClassificationModel.load(local_model_path)
    
    # Load metadata
    with open(local_metadata_path, "r") as f:
        metadata = json.load(f)
    
    print("✅ Model loaded from MinIO.")
    print(f"✅ Model metadata: {metadata}")
    return model, metadata


# Predict New Data
def predict_new_data(model, metadata, new_data_df):
    # Recreate assembler from metadata
    assembler = VectorAssembler(
        inputCols=metadata["feature_columns"],
        outputCol="features"
    )
    
    # Transform features
    assembled_data = assembler.transform(new_data_df)
    
    # Make predictions
    return model.transform(assembled_data)


# Predict and Send to Kafka
def process_and_predict(consumer, producer):
    # Load model from MinIO
    model, metadata = load_model_from_minio()
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("PySpark with Streaming Data Storing") \
        .config("spark.jars", "/tmp/postgresql-42.7.5.jar") \
        .getOrCreate()

    for message in consumer:
        data = message.value  # Read Kafka message (JSON format)
        
        bookingID = data["bookingID"]  # Extract bookingID
        records = data["records"]  # Extract multiple records
        
        data_batch = []  # Initialize a list to store records
        for record in records:
            # Add bookingID to each record
            record["bookingID"] = bookingID
            data_batch.append(record)

        # Convert data to DataFrame
        df = spark.createDataFrame(data_batch)

        # Compute required statistics
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


        prediction = predict_new_data(model, metadata, processed_df.drop("bookingID"))  # Assuming SparkML model

        # Send prediction to Kafka
        prediction_message = {
            "bookingID": bookingID,
            "time": processed_df.select("time").first()[0],
            "label": int(prediction)
        }
        producer.send(KAFKA_TOPIC_OUTPUT, prediction_message)

        print(f"✅ Sent prediction: {prediction_message}")

    print("Finished processing and predicting.")
        
def create_consumer(broker, topic):
    try:
        print(f"Attempting to connect to Kafka broker: {broker} under topic: {topic}")
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            auto_offset_reset='earliest',  # Read messages from the beginning of the topic
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserialize bytes into JSON
        )
        print("Kafka consumer created.")
        return consumer
    except Exception as e:
        print(f"Error connecting to Kafka broker: {e}")
        
def create_producer(broker):
    try:
        print(f"Attempting to connect to Kafka broker: {broker}")
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON to bytes
        )
        print("Kafka producer created.")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka broker: {e}")
        
def consume_messages(consumer, producer):
    try:
        for message in consumer:
            print(f"Received message: {message.value} from topic: {message.topic}")
            # Process the message here
            process_and_predict(consumer, producer)
    except KeyboardInterrupt:
        print("Stopped consuming messages")
    finally:
        consumer.close()

# Start Streaming Data Processing
if __name__ == "__main__":
    print("Listening for streaming data...")
    consumer = create_consumer(KAFKA_BROKER, KAFKA_TOPIC_INPUT)
    producer = create_producer(KAFKA_BROKER)
    consume_messages(consumer, producer)
    