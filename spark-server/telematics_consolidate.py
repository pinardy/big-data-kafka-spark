import os, uvicorn
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pydantic import BaseModel
from fastapi import FastAPI


FAST_API_PORT = int(os.environ.get("FAST_API_CONSOLIDATE_PORT", 8003))

MINIO_ADDRESS = os.environ["MINIO_ADDRESS"]
MINIO_PORT = os.environ["MINIO_PORT"]
MINIO_USER = os.environ["MINIO_USER"]
MINIO_PASSWORD = os.environ["MINIO_PASSWORD"]

POSTGRES_ADDRESS = os.environ["POSTGRES_ADDRESS"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]


db_connection_url = f"jdbc:postgresql://{POSTGRES_ADDRESS}:{POSTGRES_PORT}/postgres"
db_properties = {
    "user": f"{POSTGRES_USER}",
    "password": f"{POSTGRES_PASSWORD}",
    "driver": "org.postgresql.Driver"
}

def removeDuplicate(df):
    a_counts = df.groupBy("bookingID").agg(F.count("*").alias("cnt"))

    unique_a = a_counts.filter(F.col("cnt") == 1).select("bookingID")

    return df.join(unique_a, on="bookingID", how="inner")


def telematics_consolidation(command):

    print(f"====================START OF {command}====================")

    # READ LABELS FROM CSV
    spark = SparkSession.builder \
        .appName("PySpark with MinIO") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ADDRESS}:{MINIO_PORT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .getOrCreate()


    labels_df = spark.read.csv(f"s3a://bigdata/labels.csv", header=True, inferSchema=True)
    labels_df = removeDuplicate(labels_df)

    # READ data from postgres, either do ALL or NEW, then consolidate them
    ##UPDATE 2: doing ALL might have prolem due to the need to change the table when doing .write.jdbc, thus disable ALL
    sql ='(SELECT * FROM telematics_raw WHERE telematics_raw.bookingid NOT IN (SELECT telematics.bookingid FROM telematics)) AS telematics_todo'
    postgres_df = spark.read.jdbc(
        url=db_connection_url,
        table=sql,
        properties=db_properties
    )
    
    # Feature engineering
    
    # Compute magnitudes
    df = postgres_df.withColumn("accel_mag", F.sqrt(F.col("acceleration_x")**2 +
                                                F.col("acceleration_y")**2 +
                                                F.col("acceleration_z")**2)) \
                .withColumn("gyro_mag", F.sqrt(F.col("gyro_x")**2 +
                                               F.col("gyro_y")**2 +
                                               F.col("gyro_z")**2))
           
    # Aggregate per interval
    aggregated_df = df.groupBy("bookingid").agg(
        F.mean("speed").alias("avg_speed"),
        F.stddev("speed").alias("std_speed"),
        
        F.mean("accel_mag").alias("avg_accel_mag"),
        F.max("accel_mag").alias("max_accel_mag"),
        F.stddev("accel_mag").alias("std_accel_mag"),
        
        F.mean("gyro_mag").alias("avg_gyro_mag"),
        F.stddev("gyro_mag").alias("std_gyro_mag"),
        
        F.mean("acceleration_x").alias("avg_accel_x"),
        F.stddev("acceleration_x").alias("std_accel_x"),
        F.max("acceleration_x").alias("max_accel_x"),
        
        F.mean("acceleration_y").alias("avg_accel_y"),
        F.stddev("acceleration_y").alias("std_accel_y"),
        F.max("acceleration_y").alias("max_accel_y"),
        
        F.mean("acceleration_z").alias("avg_accel_z"),
        F.stddev("acceleration_z").alias("std_accel_z"),
        F.max("acceleration_z").alias("max_accel_z"),
        
        F.mean("gyro_x").alias("avg_gyro_x"),
        F.stddev("gyro_x").alias("std_gyro_x"),
        
        F.mean("gyro_y").alias("avg_gyro_y"),
        F.stddev("gyro_y").alias("std_gyro_y"),
        
        F.mean("gyro_z").alias("avg_gyro_z"),
        F.stddev("gyro_z").alias("std_gyro_z"),
        
        F.mean("accuracy").alias("avg_accuracy"),
        F.stddev("accuracy").alias("std_accuracy"),
        
        F.max("second").alias("second"),
    )
    
    # Combine label to aggregated_df
    aggregated_df.show()
    df_combined = aggregated_df.join(labels_df, "bookingid", "left")
    
    df_combined = df_combined.fillna(0.0)

    returnString = f"Completed {command} - {df_combined.count()} record done"
    ## read to telematics
    df_combined.write.jdbc(
        url=db_connection_url,
        table="telematics",
        properties=db_properties,
        mode="append")


    print(f"===================={returnString} ====================")
    spark.stop()

    return returnString


class Item(BaseModel):
    command: str

app = FastAPI()

@app.post("/command")
async def predict(data: Item):
    result = telematics_consolidation(data.command)
    return {"status": "success", "message": f"{result}"}


if __name__ == "__main__":
    uvicorn.run("telematics_consolidate:app", host="0.0.0.0", port=FAST_API_PORT, reload=True)


