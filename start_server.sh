#!/bin/bash
set -euo pipefail

if (( $# != 0 && $# != 2 )); then
    echo "Error: provide both size limits or neither." >&2
    echo "Usage: $0 [max-log-size max-message-size]" >&2
    echo "Example: $0 1GiB 1MiB" >&2
    exit 2
fi

MAX_LOG_SIZE="${1:-1GiB}"
MAX_MESSAGE_SIZE="${2:-1MiB}"

mvn compile exec:java \
    -Dexec.mainClass=com.minikafka.server.BrokerServer \
    -Dexec.args="$MAX_LOG_SIZE $MAX_MESSAGE_SIZE"
