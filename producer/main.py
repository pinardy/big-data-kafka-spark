import os, json, asyncio
import pandas as pd
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer

app = FastAPI()

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_INGESTION_TOPIC = os.environ["KAFKA_INGESTION_TOPIC"]
KAFKA_LIVE_DATA_TOPIC = os.environ["KAFKA_LIVE_DATA_TOPIC"]

# KAFKA_BROKER = "localhost:9092"
# KAFKA_INGESTION_TOPIC = "streaming"
# KAFKA_LIVE_DATA_TOPIC = "live"

data_file_path = "./part-00000-e6120af0-10c2-4248-97c4-81baf4304e5c-c000.csv"

# Simulating streaming data from a CSV file (using pandas)
df = pd.read_csv(data_file_path)

# Sort data by "bookingID" and "second" for grouping trips in order of occurrence
df = df.sort_values(by=["bookingID", "second"])

# Group by "bookingID" and collect the records as a list of dictionaries
grouped_data = (
    df.groupby("bookingID")
    .apply(lambda x: x.to_dict(orient="records"))
    .to_dict()
)


def create_producer(broker):
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize messages to JSON
    )
    return producer


async def send_message(producer, topic, message):
    try:
        print(f"Sent: bookingID: {message.get('bookingID')}, 'second': {message.get('second')}", flush=True)

        # For demo purposes of live streaming of bookingID 0 to backend
        if (message.get('bookingID') == 0):
            await asyncio.to_thread(producer.send, KAFKA_LIVE_DATA_TOPIC, message)

        # For data ingestion
        await asyncio.to_thread(producer.send, topic, message)

    except Exception as e:
        print(f"Failed to send message: {e}")


async def stream_booking_trips(producer, trips):
    for trip in trips:
        await send_message(producer, KAFKA_INGESTION_TOPIC, trip)
        await asyncio.sleep(2)


# Run "curl -X POST http://localhost:8001/stream_trips_demo" to start streaming data to Kafka
@app.post("/stream_trips_demo")
async def stream_trips():
    """
    Streams trip data to Kafka topic to simulate 3 drivers sending trip data in real-time
    """
    try:
        producer = create_producer(KAFKA_BROKER)

        tasks = []
        for i, (booking_id, trips) in enumerate(grouped_data.items()):
            task = asyncio.create_task(stream_booking_trips(producer, trips))
            tasks.append(task)

            if i == 2:
                break

        # Run all tasks concurrently
        await asyncio.gather(*tasks)
        return { "message": "Streaming to Kafka completed successfully" }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Run "curl -X POST http://localhost:8001/stream_trips" to start streaming data to Kafka
@app.post("/stream_trips")
async def stream_trips():
    """
    Streams trip data to Kafka topic to simulate 20000 drivers sending trip data in real-time
    """
    try:
        producer = create_producer(KAFKA_BROKER)

        tasks = []
        for i, (booking_id, trips) in enumerate(grouped_data.items()):
            task = asyncio.create_task(stream_booking_trips(producer, trips))
            tasks.append(task)

        # Run all tasks concurrently
        await asyncio.gather(*tasks)
        return { "message": "Streaming to Kafka completed successfully" }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# Shutdown hook to close the Kafka producer
@app.on_event("shutdown")
def shutdown_event(producer: KafkaProducer):
    producer.close()