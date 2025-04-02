import os, json
from kafka import KafkaConsumer

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = "predictions" # os.environ["KAFKA_TOPIC"]


def create_consumer(broker, topic):
    try:
        print("Attempting to connect to Kafka broker")
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
    except KeyboardInterrupt:
        print("Stopped consuming messages")
    finally:
        consumer.close()


if __name__ == "__main__":
    consumer = create_consumer(KAFKA_BROKER, KAFKA_TOPIC)
    consume_messages(consumer)