#!/usr/bin/env bash

if [ $# -ne 4 ]; then
    echo "usage: lshClusters.sh MINHASH_SIZE NUM_BANDS DOC_SEQUENCE OUTPUT_DIR"
    exit 1
fi

spark-submit --class com.stevens.spark.LSHClusters target/minhash-document-clusters-1.0-SNAPSHOT.jar $1 $2 $3 $4

exit 0
