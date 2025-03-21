import os
from pyspark.sql import SparkSession

MINIO_ADDRESS="minio"
MINIO_PORT=9000
MINIO_USER="miniouser"
MINIO_PASSWORD="miniopassword"

POSTGRES_ADDRESS="postgres_db"
POSTGRES_PORT=5432
POSTGRES_USER="admin"
POSTGRES_PASSWORD="password"



print("Starting PySpark with MinIO")

    # Create a Spark session
def minio_to_postgres(filepath):
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
    df = spark.read.csv(filepath, header=True, inferSchema=True)

    # Define PostgreSQL connection properties
    jdbc_url = f"jdbc:postgresql://{POSTGRES_ADDRESS}:{POSTGRES_PORT}/postgres"
    connection_properties = {
        "user": f"{POSTGRES_USER}",
        "password": f"{POSTGRES_PASSWORD}",
        "driver": "org.postgresql.Driver"
    }

    # Write data into PostgreSQL table
    df.write.jdbc(url=jdbc_url, table="raw_data", mode="overwrite", properties=connection_properties)


    spark.stop()



def main():
    minio_to_postgres("s3a://bigdata/raw/part-*.csv")

if __name__ == "__main__":
    main()