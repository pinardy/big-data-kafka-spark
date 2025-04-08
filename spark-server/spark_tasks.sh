#!/bin/bash

spark-submit batch_processing.py > /tmp/batch_processing.log &

spark-submit telematics_consolidate.py > /tmp/telematics_consolidate.log &

spark-submit streaming_data_handling.py > /tmp/streaming_data_handling.log &