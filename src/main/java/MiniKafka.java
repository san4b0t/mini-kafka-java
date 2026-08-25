package main.java;

import java.util.ArrayList;
import java.util.List;

public class MiniKafka {

    private final List<String> log = new ArrayList<>();

    public synchronized int produce(String message) {
        log.add(message);
        int currentOffset = log.size() - 1;
        System.out.println("Received submission: " + message + "(Offset : " + currentOffset + ")");

        notifyAll();

        return currentOffset;
    }

    public synchronized String consume(int offset) throws InterruptedException {
        while (offset >= log.size()) {
            wait();
        }

        return log.get(offset);
    }

    public static void main(String[] args) {
        MiniKafka broker = new MiniKafka();

        Thread consumerThread = new Thread(() -> {
            int myOffset = 0;

            try {
                while (true) {
                    System.out.println("Consumer waiting for offset: " + myOffset);
                    String data = broker.consume(myOffset);

                    System.out.println("Processed: " + data);
                    myOffset++;

                    Thread.sleep(3000); //simulate heavy task
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        consumerThread.start();

        Thread producerThread = new Thread(() -> {
           try {
               Thread.sleep(1000);

               String[] labs = {"Lab1_GraphTraversal", "Lab2_NetworkBootstrapping", "Lab3_LinearAlgebra"};
               for (String lab : labs) {
                   int offSet = broker.produce(lab);
                   System.out.println("Produced: " + offSet);
                   Thread.sleep(500);
               }
           } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
           }
        });

        producerThread.start();
    }
}