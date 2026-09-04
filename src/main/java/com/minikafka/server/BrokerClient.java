package com.minikafka.client;

import com.minikafka.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Scanner;

public class BrokerClient {
    private final MiniKafkaServiceGrpc.MiniKafkaServiceBlockingStub stub;

    public BrokerClient(String target) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        this.stub = MiniKafkaServiceGrpc.newBlockingStub(channel);
    }

    public void startConsumer() {
        new Thread(() -> {
            long currentOffset = 0;
            while (true) {
                ConsumeRequest request = ConsumeRequest.newBuilder().setOffset(currentOffset).build();
                ConsumeResponse response = stub.consume(request);

                if (!response.getMessage().isEmpty()) {
                    System.out.println("\n[Consumed] " + response.getMessage());
                    currentOffset = response.getNextOffset();
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }).start();
    }

    public static void main(String[] args) {
        BrokerClient client = new BrokerClient("localhost:9092");
        client.startConsumer();

        Scanner scanner = new Scanner(System.in);
        System.out.println("Client Connected. Type messages to send to the Broker Server:");

        while (true) {
            String input = scanner.nextLine();
            if (input.equalsIgnoreCase("exit"))
                break;

            ProduceRequest request = ProduceRequest.newBuilder().setMessage(input).build();
            stub.produce(request);
        }
        System.exit(0);
    }
}