#!/usr/bin/env bash

if [ $# -ne 5 ]; then
    echo "usage: lshClusters.sh SHINGLE_SIZE MINHASH_SIZE NUM_BANDS DOC_SEQUENCE OUTPUT_DIR"
    exit 1
fi

spark-submit --master local[*] --class com.stevens.spark.LSHClusters target/minhash-document-clusters-1.0-SNAPSHOT.jar $1 $2 $3 $4 $5

exit 0
