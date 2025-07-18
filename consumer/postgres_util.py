import os, psycopg2

def get_connection():
    conn = psycopg2.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"]
    )
    conn.autocommit = True
    return conn, conn.cursor()


def ingest_raw_data(conn, cursor, input: dict):
    try:
        query = """
        INSERT INTO telematics_raw (bookingid, accuracy, bearing, acceleration_x, acceleration_y, acceleration_z, gyro_x, gyro_y, gyro_z, second, speed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        values = (
            input["bookingid"],
            input["accuracy"],
            input["bearing"],
            input["acceleration_x"],
            input["acceleration_y"],
            input["acceleration_z"],
            input["gyro_x"],
            input["gyro_y"],
            input["gyro_z"],
            input["second"],
            input["speed"]
        )
        cursor.execute(query, values)  # Convert JSON to string for insertion
        conn.commit()

        print(f"Data ingested into PostgreSQL successfully: bookingid: {input['bookingid']}, second: {input['second']}")
        print("------------------------------------------------")
    except Exception as e:
        print(f"Error ingesting data into PostgreSQL: {e}")