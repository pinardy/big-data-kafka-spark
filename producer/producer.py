import os, json, time
from kafka import KafkaProducer

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

# For running script locally
# KAFKA_BROKER = "localhost:9092"
# KAFKA_TOPIC = "test"

def create_producer(broker):
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize messages to JSON
    )
    return producer


def send_message(producer, topic, message):
    try:
        producer.send(topic, message)
        print(f"Message sent to topic '{topic}': {message}")

    except Exception as e:
        print(f"Failed to send message: {e}")


if __name__ == "__main__":
    time.sleep(10) # Start the producer after the consumer
    producer = create_producer(KAFKA_BROKER)
    messages = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35}
    ]
    for message in messages:
        send_message(producer, KAFKA_TOPIC, message)
        time.sleep(1)

    print("after sending messages")
    producer.flush()  # Ensure all messages are sent before closing
    print("flushing messages")
    producer.close()
    print("closing producer")