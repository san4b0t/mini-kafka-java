# Mini Kafka (Java + gRPC)

[![CI](https://github.com/san4b0t/mini-kafka-java/actions/workflows/ci.yml/badge.svg)](https://github.com/san4b0t/mini-kafka-java/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-17%2B-ED8B00.svg?logo=openjdk&logoColor=white)
![Maven](https://img.shields.io/badge/Maven-3.9%2B-C71A36.svg?logo=apachemaven&logoColor=white)

A small Kafka-style message queue built in Java with append-only storage, byte offsets, gRPC, concurrent producers, durability, and back-pressure.

The current version runs as one broker with one persistent log. It does not yet implement topics, partitions, replication, consumer groups, or a multi-broker cluster.

## Architecture and design

- **Transport:** A plaintext gRPC server listens on port `9092` and exposes `Produce` and `Consume` RPCs.
- **Storage:** Messages are stored sequentially in `data/server_log.dat` using an append-only binary format.
- **Offsets:** An offset is the byte position of a record in the log, not a message sequence number.
- **Durability:** Every append calls `FileChannel.force(false)` before returning to flush message data to storage.
- **Concurrency:** `AppendOnlyLog.append()` and `read()` are synchronized. Concurrent producer calls cannot overlap updates to the file or its write offset.
- **Capacity:** The broker limits both the total physical log size and the UTF-8 byte size of an individual message.
- **Consumption:** The included client polls from offset `0` and advances using the `next_offset` returned by the broker.

### Storage format

Each record contains a four-byte length followed by the UTF-8 message payload:

```text
+----------------------+------------------------+
| payload length (4 B) | UTF-8 message (N bytes)|
+----------------------+------------------------+
^ record offset
```

The next record begins at:

```text
next_offset = current_offset + 4 + payload_byte_length
```

## Requirements

- Java 17 or newer
- Maven 3.9 or newer
- Bash for the convenience scripts

## How to run locally

Clone the repository and create the ignored data directory:

```bash
git clone https://github.com/san4b0t/mini-kafka-java.git
cd mini-kafka-java
mkdir -p data
```

Start the broker with its default limits:

```bash
./start_server.sh
```

The defaults are:

| Setting | Default | Meaning |
| :--- | :--- | :--- |
| Maximum log size | `1GiB` | Maximum size of `data/server_log.dat`, including four-byte record headers |
| Maximum message size | `1MiB` | Maximum UTF-8 payload size for one produced message |

To customize the limits, provide both values in this order:

```bash
./start_server.sh 2GiB 512KiB
```

Supported units are `B`, `KiB`, `MiB`, and `GiB`. A value without a suffix is interpreted as bytes. Supplying only one limit is rejected before Maven starts.

In a second terminal, start the interactive producer and consumer client:

```bash
./start_client.sh
```

Type a message and press Enter to produce it. The background consumer prints messages as it reads them. Enter `exit` to stop the client.

## Running tests

Run the complete test suite:

```bash
mvn test
```

The tests cover:

- sequential append and offset calculation;
- UTF-8 payloads;
- reopening an existing log;
- log-capacity enforcement;
- broker produce and consume behavior;
- empty, blank, and oversized message rejection;
- concurrent append integrity and performance metrics.

### Concurrency and load test

`AppendOnlyLogConcurrencyTest` starts `16` worker threads and appends `5,100` messages by default. It fails if records are lost, corrupted, assigned duplicate offsets, or if the executor cannot finish safely.

Run only that test:

```bash
mvn -Dtest=AppendOnlyLogConcurrencyTest test
```

Customize the workload:

```bash
mvn -Dload.messages=10000 \
    -Dload.threads=32 \
    -Dtest=AppendOnlyLogConcurrencyTest test
```

The report is printed to the terminal and written to:

```text
target/metrics/append-only-log-load.txt
```

Reported metrics include:

- total messages, threads, errors, unique offsets, and bytes written;
- elapsed time;
- messages per second and MiB per second;
- average, p50, p95, p99, and maximum append latency.

Performance values are intentionally not compared with a strict threshold because disk and CI-runner performance varies. The test enforces correctness and a two-minute safety timeout.

## gRPC API examples

The interactive client is the easiest way to use the broker. You can also call it with [`grpcurl`](https://github.com/fullstorydev/grpcurl) by supplying the project proto file.

### Produce a message

```bash
grpcurl -plaintext \
    -import-path src/main/proto \
    -proto minikafka.proto \
    -d '{"message":"Hello from gRPC"}' \
    localhost:9092 minikafka.MiniKafkaService/Produce
```

Example response:

```json
{
  "offset": "0"
}
```

### Consume from an offset

```bash
grpcurl -plaintext \
    -import-path src/main/proto \
    -proto minikafka.proto \
    -d '{"offset":"0"}' \
    localhost:9092 minikafka.MiniKafkaService/Consume
```

Example response:

```json
{
  "message": "Hello from gRPC",
  "nextOffset": "19"
}
```

## API specification

| RPC | Request | Response | Behavior and status codes |
| :--- | :--- | :--- | :--- |
| `Produce` | `message: string` | `offset: int64` | Appends a message. Returns `INVALID_ARGUMENT` for blank or oversized input, `RESOURCE_EXHAUSTED` when the log is full, and `INTERNAL` on storage failure. |
| `Consume` | `offset: int64` | `message: string`, `next_offset: int64` | Reads the record at a byte offset. Returns an empty response when no message currently exists at that offset. |

The protobuf contract is defined in [`src/main/proto/minikafka.proto`](src/main/proto/minikafka.proto).

## Validation and capacity behavior

The broker rejects:

1. empty messages;
2. whitespace-only messages;
3. messages larger than the configured UTF-8 byte limit;
4. an append that would make the physical log exceed its configured capacity.

The capacity check and append occur inside the same synchronized method, so concurrent producers cannot individually pass the check and collectively overfill the log. Rejected records are not partially written.

An existing log that is already larger than a newly configured limit prevents the broker from starting. Because retention and log rotation are not implemented yet, a full log remains full until the limit is increased or the data is deliberately removed.

## Concurrency and thread safety

Without synchronization, multiple producers could receive the same offset or interleave record bytes. `AppendOnlyLog` prevents this by serializing the capacity check, positional write, durability flush, and write-offset update in `append()`.

Reads are also synchronized and use positional `FileChannel` reads, so they do not mutate the channel's shared position. The implementation loops until an entire record has been transferred, which protects against partial file reads or writes.

This design favors simple correctness and durability over maximum throughput. In particular, producers wait for the append lock and every record is flushed individually. Batching multiple records per flush would be a useful future optimization.

## Current topology and scope

This project currently has a single topology:

```text
Interactive client(s)
        |
        | gRPC :9092
        v
   BrokerServer
        |
        v
 AppendOnlyLog
        |
        v
data/server_log.dat
```

Possible future extensions include topics, partitions, consumer groups, log segments and retention, graceful client shutdown, server metrics endpoints, replication, and broker discovery.

## CI/CD automation

The GitHub Actions workflow in [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on:

- every push to every branch;
- every pull request, regardless of its target branch;
- manual workflow dispatches.

CI uses Java 17 and runs:

```bash
mvn --batch-mode --no-transfer-progress verify
```

The test reports and concurrency metrics are uploaded as a workflow artifact and retained for 14 days. The concurrency report is also displayed in the GitHub Actions job summary.

## Project structure

```text
.
├── .github/workflows/ci.yml
├── pom.xml
├── start_client.sh
├── start_server.sh
└── src
    ├── main
    │   ├── java/com/minikafka
    │   │   ├── client/BrokerClient.java
    │   │   └── server
    │   │       ├── AppendOnlyLog.java
    │   │       ├── BrokerServer.java
    │   │       └── LogCapacityExceededException.java
    │   └── proto/minikafka.proto
    └── test/java/com/minikafka/server
        ├── AppendOnlyLogConcurrencyTest.java
        ├── AppendOnlyLogTest.java
        └── BrokerServerTest.java
```

## Command summary

```bash
# Start with 1 GiB log and 1 MiB message defaults
mkdir -p data
./start_server.sh

# Start with custom limits
./start_server.sh 2GiB 512KiB

# Run the client in another terminal
./start_client.sh

# Run every test
mvn test

# Run a custom concurrency test
mvn -Dload.messages=10000 -Dload.threads=32 \
    -Dtest=AppendOnlyLogConcurrencyTest test
```
