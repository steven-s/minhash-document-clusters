#!/usr/bin/env bash

if [ $# -ne 2 ]; then
    echo "usage: textToSequence.sh TEXT_FILE_DIR OUTPUT_SEQUENCE"
    exit 1
fi

java -cp target/minhash-document-clusters-1.0-SNAPSHOT.jar com.stevens.transform.TextToSequence $1 $2

exit 0
