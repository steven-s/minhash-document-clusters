#!/usr/bin/env bash

scala -cp target/minhash-document-clusters-1.0-SNAPSHOT.jar com.stevens.transform.TextToSequence $1 $2

exit 0
