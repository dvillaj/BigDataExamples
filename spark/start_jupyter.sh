#!/bin/bash

export PYSPARK_DRIVER_PYTHON="$HOME/miniconda2/bin/jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

pyspark --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/test.myCollection?readPreference=primaryPreferred" \
        --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/test.myCollection" \
        --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.1 \
        --packages com.databricks:spark-csv_2.10:1.5.0