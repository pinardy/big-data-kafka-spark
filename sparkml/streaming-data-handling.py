from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaConsumer, KafkaProducer
import os, json

import psycopg2
from pyspark.ml.pipeline import PipelineModel
from minio import Minio

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

    # Download the model file from MinIO
    local_model_path = "/tmp/sparkml_model"
    minio_client.fget_object(bucket_name, model_path, local_model_path)

    # Load the Spark ML model
    model = PipelineModel.load(local_model_path)
    print("✅ Model loaded from MinIO.")
    return model


# Store Streaming Data in PostgreSQL
def store_streaming_data(data, spark):
    # Define the schema for the data
    schema = ["bookingID", "Accuracy", "Bearing", "acceleration_x", "acceleration_y", 
              "acceleration_z", "gyro_x", "gyro_y", "gyro_z", "second", "Speed"]

    # Create a DataFrame from the data
    df = spark.createDataFrame(data, schema=schema)

    # Write the DataFrame to PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{POSTGRES_ADDRESS}:{POSTGRES_PORT}/telematics") \
        .option("dbtable", "streaming_data") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print("✅ Streaming data stored in PostgreSQL.")


# Predict and Send to Kafka
def process_and_predict(consumer, producer):
    # Load model from MinIO
    model = load_model_from_minio()
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("PySpark with Streaming Data Storing") \
        .config("spark.jars", "/tmp/postgresql-connector.jar") \
        .getOrCreate()

    for message in consumer:
        data = message.value

        # Store received data
        store_streaming_data([(data["bookingID"], data["Accuracy"], data["Bearing"], data["acceleration_x"],
                               data["acceleration_y"], data["acceleration_z"], data["gyro_x"], data["gyro_y"], 
                               data["gyro_z"], data["second"], data["Speed"])], spark)

        # Prepare data for prediction
        feature_vector = [
            data["Speed"], data["acceleration_x"], data["acceleration_y"], 
            data["acceleration_z"], data["gyro_x"], data["gyro_y"], data["gyro_z"]
        ]
        prediction = model.transform([feature_vector])[0]

        # Send prediction to frontend via Kafka
        prediction_message = {"bookingID": data["bookingID"], "second": data["second"], "label": int(prediction)}
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
        print("✅ Kafka consumer created.")
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
        print("✅ Kafka producer created.")
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