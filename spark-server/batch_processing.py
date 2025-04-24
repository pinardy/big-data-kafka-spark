import os, uvicorn
from pyspark.sql import SparkSession
from pydantic import BaseModel
from fastapi import FastAPI
from pyspark.sql.functions import col

FAST_API_PORT = int(os.environ.get("FAST_API_BATCH_PORT", 8002))

MINIO_ADDRESS = os.environ["MINIO_ADDRESS"]
MINIO_PORT = os.environ["MINIO_PORT"]
MINIO_USER = os.environ["MINIO_USER"]
MINIO_PASSWORD = os.environ["MINIO_PASSWORD"]

POSTGRES_ADDRESS = os.environ["POSTGRES_ADDRESS"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

print("Starting PySpark with MinIO")

# Create a Spark session
def minio_to_postgres(filepath):
    print(f"====================START OF handling {filepath}====================")
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

    df = df.select([col(c).alias(c.lower()) for c in df.columns])

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

    print(f"====================COMPLETED of {filepath} ====================")

    return f"File {filepath} processed successfully."

app = FastAPI()

class Item(BaseModel):
    filepath: str

@app.post("/filepath")
async def predict(data: Item):
    result = minio_to_postgres(data.filepath)
    return {"status": "success", "message": f"{result}"}

if __name__ == "__main__":
    uvicorn.run("batch_processing:app", host="0.0.0.0", port=FAST_API_PORT, reload=True)
