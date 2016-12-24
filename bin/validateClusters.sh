#!/usr/bin/env bash

if [ $# -ne 3 ]; then
    echo "usage: validateClusters.sh SHINGLE_LENGTH TEXT_FILE_DIR CLUSTER_FILE"
    exit 1
fi

java -cp target/minhash-document-clusters-1.0-SNAPSHOT.jar com.stevens.minhash.ValidateClusters $1 $2 $3

exit 0
