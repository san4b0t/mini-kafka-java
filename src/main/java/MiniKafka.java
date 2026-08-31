package main.java;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.Semaphore;

public class MiniKafka {
    private final FileChannel channel;
    private long producerOffset = 0;
    private long consumerOffset = 0;

    private final Semaphore spaces = new Semaphore(50);
    private final Semaphore items = new Semaphore(0);
    private final Semaphore mutex = new Semaphore(1);

    public MiniKafka(String filePath) {
        try {
            File file = new File(filePath);

            if (file.createNewFile()) {
                System.out.println("Created NEW log file at: " + file.getAbsolutePath());
            } else {
                System.out.println("Opened EXISTING log file at: " + file.getAbsolutePath());
            }

            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            this.channel = raf.getChannel();
            this.producerOffset = channel.size();
            this.consumerOffset = 0;
            System.out.println("Storage initialized. Ready to start buffer simulation.\n");
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize storage", e);
        }
    }

    /**
     * PRODUCER API
     */
    public long produce(String message) throws InterruptedException {

        spaces.acquire();
        mutex.acquire();

        long offset = producerOffset;
        try {
            byte[] data = message.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
            buffer.putInt(data.length);
            buffer.put(data);
            buffer.flip();

            while (buffer.hasRemaining()) {
                channel.write(buffer, offset);
            }
            channel.force(false);

            producerOffset += 4 + data.length;
            System.out.println("Saved to disk: " + message + " (Offset: " + offset + ")");
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to disk", e);
        } finally {
            mutex.release();
        }

        items.release();
        return offset;
    }

    /**
     * CONSUMER API
     */
    public String consume() throws InterruptedException {
        items.acquire();
        mutex.acquire();

        String message;
        try {
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            channel.read(lengthBuffer, consumerOffset);
            lengthBuffer.flip();
            int length = lengthBuffer.getInt();

            ByteBuffer dataBuffer = ByteBuffer.allocate(length);
            channel.read(dataBuffer, consumerOffset + 4);
            dataBuffer.flip();

            message = new String(dataBuffer.array(), StandardCharsets.UTF_8);
            consumerOffset += 4 + length;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read from disk", e);
        } finally {
            mutex.release();
        }

        spaces.release();

        return message;
    }

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Error: No file path provided.");
            System.err.println("Usage: java main.java.MiniKafka <filepath>");
            System.exit(1);
        }

        String filePath = args[0];
        MiniKafka broker = new MiniKafka(filePath);

        Thread consumerThread = new Thread(() -> {
            try {
                while (true) {
                    String data = broker.consume();
                    System.out.println("Processed from disk: " + data);
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        consumerThread.setDaemon(true);
        consumerThread.start();

        Scanner scanner = new Scanner(System.in);
        System.out.println("Producer ready. Enter submission files to queue for grading (type 'exit' to quit):");

        try {
            while (true) {
                String input = scanner.nextLine();
                if (input.equalsIgnoreCase("exit")) {
                    break;
                }
                broker.produce(input);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            scanner.close();
            System.out.println("Exiting broker...");
        }
    }
}