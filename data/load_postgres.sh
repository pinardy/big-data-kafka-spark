#!/bin/bash

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\q' 2>/dev/null; do
  sleep 2
done


echo "Creating telematics_raw table"
PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<EOF
CREATE TABLE IF NOT EXISTS telematics_raw (
    bookingid BIGINT,
    accuracy FLOAT,
    bearing FLOAT,
    acceleration_x FLOAT,
    acceleration_y FLOAT,
    acceleration_z FLOAT,
    gyro_x FLOAT,
    gyro_y FLOAT,
    gyro_z FLOAT,
    second FLOAT,
    speed FLOAT,

    PRIMARY KEY (bookingid, second)
);
EOF

echo "Creating telematics table"
PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<EOF
CREATE TABLE IF NOT EXISTS telematics (
    bookingid BIGINT,
    avg_speed FLOAT NOT NULL,
    std_speed FLOAT NOT NULL,
    avg_accel_mag FLOAT NOT NULL,
    max_accel_mag FLOAT NOT NULL,
    std_accel_mag FLOAT NOT NULL,
    avg_gyro_mag FLOAT NOT NULL,
    std_gyro_mag FLOAT NOT NULL,
    avg_accel_x FLOAT NOT NULL,
    std_accel_x FLOAT NOT NULL,
    max_accel_x FLOAT NOT NULL,
    avg_accel_y FLOAT NOT NULL,
    std_accel_y FLOAT NOT NULL,
    max_accel_y FLOAT NOT NULL,
    avg_accel_z FLOAT NOT NULL,
    std_accel_z FLOAT NOT NULL,
    max_accel_z FLOAT NOT NULL,
    avg_gyro_x FLOAT NOT NULL,
    std_gyro_x FLOAT NOT NULL,
    avg_gyro_y FLOAT NOT NULL,
    std_gyro_y FLOAT NOT NULL,
    avg_gyro_z FLOAT NOT NULL,
    std_gyro_z FLOAT NOT NULL,
    avg_accuracy FLOAT NOT NULL,
    std_accuracy FLOAT NOT NULL,
    second FLOAT,
    label INTEGER,

    PRIMARY KEY (bookingid, second)
);
EOF

# Load data from CSV (skipping header)
echo "Table Created Complete"