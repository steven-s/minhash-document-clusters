#!/usr/bin/env bash

if [ $# -ne 3 ]; then
    echo "usage: shingleCounter.sh SHINGLE_LENGTH DOC_SEQUENCE OUTPUT_DIR"
    exit 1
fi

spark-submit --master local[*] --class com.stevens.spark.ShingleCounter target/minhash-document-clusters-1.0-SNAPSHOT.jar $1  $2 $3

exit 0
