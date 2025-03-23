#!/bin/bash

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\q' 2>/dev/null; do
  sleep 2
done

echo "Creating telematics_raw table"
PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<EOF
CREATE TABLE IF NOT EXISTS telematics_raw (
    bookingID BIGINT PRIMARY KEY,
    Accuracy FLOAT,
    Bearing FLOAT,
    acceleration_x FLOAT,
    acceleration_y FLOAT,
    acceleration_z FLOAT,
    gyro_x FLOAT,
    gyro_y FLOAT,
    gyro_z FLOAT,
    second FLOAT,
    Speed FLOAT
);
EOF

echo "Creating telematics table"
PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<EOF
CREATE TABLE IF NOT EXISTS telematics (
    bookingID BIGINT PRIMARY KEY,
    Speed_perc70 FLOAT,
    acceleration_x_min FLOAT,
    acceleration_z_std FLOAT,
    Bearing_std FLOAT,
    acceleration_x_std FLOAT,
    Speed_std FLOAT,
    acceleration_y_std FLOAT,
    acceleration_z_max FLOAT,
    Speed_max FLOAT,
    time FLOAT,
    label INTEGER
);
EOF

# Load data from CSV (skipping header)
echo "Loading data into telematics table"
PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\COPY telematics(bookingID, Speed_perc70, acceleration_x_min, acceleration_z_std, Bearing_std, acceleration_x_std, Speed_std, acceleration_y_std, acceleration_z_max, Speed_max, time, label) FROM '/data/safety_dataset_filtered.csv' DELIMITER ',' CSV HEADER;"

echo "Data load complete"