#!/bin/bash
set -euo pipefail

if (( $# != 0 )); then
    echo "Error: start_client.sh does not accept arguments." >&2
    echo "Usage: $0" >&2
    exit 2
fi

mvn compile exec:java -Dexec.mainClass=com.minikafka.client.BrokerClient
