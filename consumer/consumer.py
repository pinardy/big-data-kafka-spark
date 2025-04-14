import os, json
from kafka import KafkaConsumer
from postgres_util import get_connection, ingest_raw_data

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = os.environ["KAFKA_INGESTION_TOPIC"]

# For running script locally
# KAFKA_BROKER = "localhost:9092"
# KAFKA_TOPIC = "streaming"

def create_consumer(broker, topic):
    try:
        print("Attempting to connect to Kafka broker")
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            auto_offset_reset='latest', # Start reading at the latest message
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserialize bytes into JSON
        )
        print("Connected to Kafka broker successfully")
        return consumer

    except Exception as e:
        print(f"Error connecting to Kafka broker: {e}")


def consume_messages(consumer):
    try:
        # Connect to PostgreSQL database
        conn, cursor = get_connection()

        for message in consumer:
            # print(f"Received message: Booking ID: {message.value['bookingid']}, Second: {message.value['second']}")
            print(f"Received message: {message.value}")
            ingest_raw_data(conn, cursor, message.value)
    except KeyboardInterrupt:
        print("Stopped consuming messages")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        consumer.close()


if __name__ == "__main__":
    consumer = create_consumer(KAFKA_BROKER, KAFKA_TOPIC)
    consume_messages(consumer)