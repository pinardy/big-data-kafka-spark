#!/bin/bash

spark-submit batch_processing.py > /tmp/batch_processing.log &

spark-submit telematics_consolidate.py > /tmp/telematics_consolidate.log &

spark-submit streaming-data-handling.py > /tmp/streaming-data-handling.log &