import os, psycopg2, json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor
from aiokafka import AIOKafkaConsumer

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_connection():
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"]
    )
    return conn, conn.cursor(cursor_factory=RealDictCursor)


@app.get("/trip/get_all")
async def get_all_trips():
    try:
        conn, cursor = get_connection()
        cursor.execute("SELECT * FROM telematics;")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        return {"error": str(e)}
    
@app.get("/trip/{trip_id}")
async def get_trip_info(trip_id):
    try:
        conn, cursor = get_connection()
        cursor.execute("SELECT * FROM telematics WHERE trip_id = %s;", (trip_id,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        return {"error": str(e)}


# Websocket for live streaming data to frontend
@app.websocket("/ws/live-data")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        consumer = AIOKafkaConsumer(
            os.environ["KAFKA_TOPIC"],
            bootstrap_servers=[os.environ["KAFKA_BROKER"]],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        await consumer.start()

        while True:
            async for message in consumer:
                await websocket.send_json(message.value)
    except WebSocketDisconnect:
        print("WebSocket client disconnected")
    finally:
        await consumer.stop()