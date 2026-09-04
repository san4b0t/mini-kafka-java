package com.minikafka.server;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.io.IOException;

public class AppendOnlyLog {
    private final FileChannel channel;
    private long writeOffset = 0;

    public AppendOnlyLog(String filePath) {
        try {
            File file = new File(filePath);
            file.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            this.channel = raf.getChannel();
            this.writeOffset = channel.size();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize storage", e);
        }
    }

    public synchronized long append(String message) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        long offset = writeOffset;

        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();

        while (buffer.hasRemaining()) {
            channel.write(buffer, offset);
        }
        channel.force(false);

        writeOffset += 4 + data.length;
        return offset;
    }

    public synchronized String read(long targetOffset) throws IOException {
        if (targetOffset >= writeOffset) {
            return null;
        }

        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        channel.read(lengthBuffer, targetOffset);
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();

        ByteBuffer dataBuffer = ByteBuffer.allocate(length);
        channel.read(dataBuffer, targetOffset + 4);
        dataBuffer.flip();

        return new String(dataBuffer.array(), StandardCharsets.UTF_8);
    }
}