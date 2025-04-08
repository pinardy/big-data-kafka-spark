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
    bookingid BIGINT PRIMARY KEY,
    speed_perc70 FLOAT,
    acceleration_x_min FLOAT,
    acceleration_z_std FLOAT,
    bearing_std FLOAT,
    acceleration_x_std FLOAT,
    speed_std FLOAT,
    acceleration_y_std FLOAT,
    acceleration_z_max FLOAT,
    speed_max FLOAT,
    time FLOAT,
    label INTEGER
);
EOF

# Load data from CSV (skipping header)
echo "Table Created Complete"