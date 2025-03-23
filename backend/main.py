import os, psycopg2
from fastapi import FastAPI
from psycopg2.extras import RealDictCursor

app = FastAPI()

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
        # Connect to the database
        conn, cursor = get_connection()

        # Query the telematics table
        cursor.execute("SELECT * FROM telematics;")
        rows = cursor.fetchall()
        print(rows)

        # Close the connection
        cursor.close()
        conn.close()

        return rows
    except Exception as e:
        return {"error": str(e)}