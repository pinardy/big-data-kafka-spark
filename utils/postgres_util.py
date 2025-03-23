
import os, psycopg2, json

def get_connection():
    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"]
    )
    return conn, conn.cursor()


def ingest_raw_data(conn, cursor, input: dict):
    try:
        query = """
        INSERT INTO telematics_raw (data) 
        VALUES (%s)
        """
        cursor.execute(query, [json.dumps(input)])  # Convert JSON to string for insertion
        conn.commit()

        print(f"Data ingested into PostgreSQL successfully: {input}")
    except Exception as e:
        print(f"Error ingesting data into PostgreSQL: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()