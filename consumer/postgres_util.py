
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
        INSERT INTO telematics_raw (bookingId, Accuracy, Bearing, acceleration_x, acceleration_y, acceleration_z, gyro_x, gyro_y, gyro_z, second, Speed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = (
            input["bookingId"],
            input["Accuracy"],
            input["Bearing"],
            input["acceleration_x"],
            input["acceleration_y"],
            input["acceleration_z"],
            input["gyro_x"],
            input["gyro_y"],
            input["gyro_z"],
            input["second"],
            input["Speed"]
        )
        cursor.execute(query, values)  # Convert JSON to string for insertion
        conn.commit()

        print(f"Data ingested into PostgreSQL successfully: {input}")
    except Exception as e:
        print(f"Error ingesting data into PostgreSQL: {e}")