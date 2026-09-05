package com.minikafka.server;

import com.minikafka.grpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BrokerServer extends MiniKafkaServiceGrpc.MiniKafkaServiceImplBase {

    static final long DEFAULT_MAX_LOG_BYTES = 1024L * 1024 * 1024;
    static final int DEFAULT_MAX_MESSAGE_BYTES = 1024 * 1024;
    private static final int GRPC_MESSAGE_OVERHEAD_BYTES = 1024;
    private static final Pattern SIZE_PATTERN = Pattern.compile("^(\\d+)(B|KiB|MiB|GiB)?$", Pattern.CASE_INSENSITIVE);

    private final AppendOnlyLog log;
    private final int maxMessageBytes;

    public BrokerServer(String logPath) {
        this(logPath, DEFAULT_MAX_LOG_BYTES, DEFAULT_MAX_MESSAGE_BYTES);
    }

    public BrokerServer(String logPath, long maxLogBytes, int maxMessageBytes) {
        if (maxMessageBytes <= 0) {
            throw new IllegalArgumentException("Maximum message size must be greater than zero");
        }
        if (maxLogBytes < Integer.BYTES + (long) maxMessageBytes) {
            throw new IllegalArgumentException(
                    "Maximum log size must fit at least one maximum-sized message");
        }

        this.log = new AppendOnlyLog(logPath, maxLogBytes);
        this.maxMessageBytes = maxMessageBytes;
        System.out.println("Broker storage mounted at: " + logPath);
        System.out.println("Maximum log size: " + maxLogBytes + " bytes");
        System.out.println("Maximum message size: " + maxMessageBytes + " bytes");
    }

    @Override
    public void produce(ProduceRequest request, StreamObserver<ProduceResponse> responseObserver) {
        String message = request.getMessage();
        if (message.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Message must not be empty or blank")
                    .asRuntimeException());
            return;
        }

        int messageBytes = message.getBytes(StandardCharsets.UTF_8).length;
        if (messageBytes > maxMessageBytes) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Message is " + messageBytes
                            + " bytes; maximum is " + maxMessageBytes + " bytes")
                    .asRuntimeException());
            return;
        }

        try {
            long offset = log.append(message);
            System.out.println("Stored: " + message + " (Offset: " + offset + ")");

            ProduceResponse response = ProduceResponse.newBuilder().setOffset(offset).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (LogCapacityExceededException e) {
            responseObserver.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        } catch (IOException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to append message")
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void consume(ConsumeRequest request, StreamObserver<ConsumeResponse> responseObserver) {
        try {
            String data = log.read(request.getOffset());
            if (data != null) {
                long nextOffset = request.getOffset() + 4 + data.getBytes().length;
                ConsumeResponse response = ConsumeResponse.newBuilder()
                        .setMessage(data)
                        .setNextOffset(nextOffset)
                        .build();
                responseObserver.onNext(response);
            } else {
                responseObserver.onNext(ConsumeResponse.newBuilder().build());
            }
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 0 && args.length != 2) {
            System.err.println("Invalid configuration: provide both size limits or neither.");
            printUsage();
            return;
        }

        long maxLogBytes;
        int maxMessageBytes;
        BrokerServer service;
        try {
            maxLogBytes = args.length == 2
                    ? parseSizeBytes(args[0])
                    : DEFAULT_MAX_LOG_BYTES;
            long parsedMessageBytes = args.length == 2
                    ? parseSizeBytes(args[1])
                    : DEFAULT_MAX_MESSAGE_BYTES;
            if (parsedMessageBytes > Integer.MAX_VALUE - GRPC_MESSAGE_OVERHEAD_BYTES) {
                throw new IllegalArgumentException("Maximum message size is too large");
            }
            maxMessageBytes = (int) parsedMessageBytes;
            service = new BrokerServer(
                    "data/server_log.dat", maxLogBytes, maxMessageBytes);
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid configuration: " + e.getMessage());
            printUsage();
            return;
        }

        Server server = ServerBuilder.forPort(9092)
                .maxInboundMessageSize(maxMessageBytes + GRPC_MESSAGE_OVERHEAD_BYTES)
                .addService(service)
                .build()
                .start();

        System.out.println("Broker Server started on port 9092.");
        server.awaitTermination();
    }

    static long parseSizeBytes(String value) {
        Matcher matcher = SIZE_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "use a positive whole number with B, KiB, MiB, or GiB");
        }

        long amount;
        try {
            amount = Long.parseLong(matcher.group(1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("size is too large", e);
        }
        if (amount == 0) {
            throw new IllegalArgumentException("size must be greater than zero");
        }

        String unit = matcher.group(2);
        long multiplier = switch (unit == null ? "B" : unit.toUpperCase(Locale.ROOT)) {
            case "B" -> 1L;
            case "KIB" -> 1024L;
            case "MIB" -> 1024L * 1024;
            case "GIB" -> 1024L * 1024 * 1024;
            default -> throw new IllegalArgumentException("unsupported size unit");
        };

        try {
            return Math.multiplyExact(amount, multiplier);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("size is too large", e);
        }
    }

    private static void printUsage() {
        System.err.println(
                "Usage: BrokerServer [max-log-size max-message-size]\n"
                        + "Example: BrokerServer 1GiB 1MiB");
    }
}
