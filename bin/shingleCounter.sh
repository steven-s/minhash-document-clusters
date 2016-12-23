#!/usr/bin/env bash

if [ $# -ne 2 ]; then
    echo "usage: shingleCounter.sh DOC_SEQUENCE OUTPUT_DIR"
    exit 1
fi

spark-submit --master local[*] --class com.stevens.spark.ShingleCounter target/minhash-document-clusters-1.0-SNAPSHOT.jar $1  $2

exit 0
