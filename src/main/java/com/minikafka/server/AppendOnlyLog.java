package com.minikafka.server;

import java.io.File;
import java.io.EOFException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.io.IOException;

public class AppendOnlyLog implements AutoCloseable {
    private final FileChannel channel;
    private final long maxLogBytes;
    private long writeOffset = 0;

    public AppendOnlyLog(String filePath) {
        this(filePath, Long.MAX_VALUE);
    }

    public AppendOnlyLog(String filePath, long maxLogBytes) {
        if (maxLogBytes <= 0) {
            throw new IllegalArgumentException("Maximum log size must be greater than zero");
        }

        try {
            File file = new File(filePath);
            file.createNewFile();
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            this.channel = raf.getChannel();
            this.writeOffset = channel.size();
            this.maxLogBytes = maxLogBytes;

            if (writeOffset > maxLogBytes) {
                channel.close();
                throw new IllegalArgumentException(
                        "Existing log size " + writeOffset
                                + " exceeds configured maximum " + maxLogBytes);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize storage", e);
        }
    }

    public synchronized long append(String message) throws IOException {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        long recordSize = Integer.BYTES + (long) data.length;

        if (recordSize > maxLogBytes - writeOffset) {
            throw new LogCapacityExceededException(maxLogBytes, writeOffset, recordSize);
        }

        long offset = writeOffset;

        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();

        long position = offset;
        while (buffer.hasRemaining()) {
            position += channel.write(buffer, position);
        }
        channel.force(false);

        writeOffset += recordSize;
        return offset;
    }

    public synchronized String read(long targetOffset) throws IOException {
        if (targetOffset >= writeOffset) {
            return null;
        }

        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        readFully(lengthBuffer, targetOffset);
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();

        ByteBuffer dataBuffer = ByteBuffer.allocate(length);
        readFully(dataBuffer, targetOffset + 4);
        dataBuffer.flip();

        return new String(dataBuffer.array(), StandardCharsets.UTF_8);
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }

    private void readFully(ByteBuffer buffer, long startPosition) throws IOException {
        long position = startPosition;
        while (buffer.hasRemaining()) {
            int bytesRead = channel.read(buffer, position);
            if (bytesRead < 0) {
                throw new EOFException("Unexpected end of log at byte " + position);
            }
            position += bytesRead;
        }
    }
}
