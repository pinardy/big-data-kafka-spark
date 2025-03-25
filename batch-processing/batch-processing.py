import os, json
from pyspark.sql import SparkSession
from kafka import KafkaConsumer

MINIO_ADDRESS = os.environ["MINIO_ADDRESS"]
MINIO_PORT = os.environ["MINIO_PORT"]
MINIO_USER = os.environ["MINIO_USER"]
MINIO_PASSWORD = os.environ["MINIO_PASSWORD"]

POSTGRES_ADDRESS = os.environ["POSTGRES_ADDRESS"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]


KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]


print("Starting PySpark with MinIO")

    # Create a Spark session
def minio_to_postgres(filepath):
    print(f"reading file : {filepath}")
    spark = SparkSession.builder \
        .appName("PySpark with MinIO") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ADDRESS}:{MINIO_PORT}")\
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    # Example: Reading from and writing to MinIO
    df = spark.read.csv(f"s3a://{filepath}", header=True, inferSchema=True)

    # Define PostgreSQL connection properties
    jdbc_url = f"jdbc:postgresql://{POSTGRES_ADDRESS}:{POSTGRES_PORT}/postgres"
    connection_properties = {
        "user": f"{POSTGRES_USER}",
        "password": f"{POSTGRES_PASSWORD}",
        "driver": "org.postgresql.Driver"
    }

    # Write data into PostgreSQL table
    df.write.jdbc(url=jdbc_url, table="telematics_raw", mode="append", properties=connection_properties)


    spark.stop()



# def main():
#     minio_to_postgres("s3a://bigdata/raw/part-*.csv")
def create_consumer(broker, topic):
    try:
        print(f"Attempting to connect to Kafka broker: {broker} under topic: {topic}")
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            auto_offset_reset='earliest',  # Read messages from the beginning of the topic
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserialize bytes into JSON
        )
        print("Connected to Kafka broker successfully")
        return consumer

    except Exception as e:
        print(f"Error connecting to Kafka broker: {e}")

def consume_messages(consumer):
    try:
        for message in consumer:
            print(f"Received message: {message.value} from topic: {message.topic}")
            minio_to_postgres(message.value['filepath'])
    except KeyboardInterrupt:
        print("Stopped consuming messages")
    finally:
        consumer.close()


if __name__ == "__main__":
    # minio_to_postgres("bigdata/raw/part-00000*.csv")
    consumer = create_consumer(KAFKA_BROKER, KAFKA_TOPIC)
    consume_messages(consumer)