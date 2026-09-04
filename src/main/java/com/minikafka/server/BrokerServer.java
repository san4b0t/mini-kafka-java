package com.minikafka.server;

import com.minikafka.grpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

public class BrokerServer extends MiniKafkaServiceGrpc.MiniKafkaServiceImplBase {

    private final AppendOnlyLog log;

    public BrokerServer(String logPath) {
        this.log = new AppendOnlyLog(logPath);
        System.out.println("Broker storage mounted at: " + logPath);
    }

    @Override
    public void produce(ProduceRequest request, StreamObserver<ProduceResponse> responseObserver) {
        try {
            long offset = log.append(request.getMessage());
            System.out.println("Stored: " + request.getMessage() + " (Offset: " + offset + ")");

            ProduceResponse response = ProduceResponse.newBuilder().setOffset(offset).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
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
                // If no data, return empty response (client will poll again)
                responseObserver.onNext(ConsumeResponse.newBuilder().build());
            }
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BrokerServer service = new BrokerServer("data/server_log.dat");
        Server server = ServerBuilder.forPort(9092)
                .addService(service)
                .build()
                .start();

        System.out.println("Broker Server started on port 9092.");
        server.awaitTermination();
    }
}