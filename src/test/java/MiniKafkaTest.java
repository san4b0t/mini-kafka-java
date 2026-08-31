package test.java;

import main.java.MiniKafka;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

public class MiniKafkaTest {

    @TempDir
    Path tempDir;

    @Test
    public void testDiskPersistenceAndRead() throws Exception {
        File logFile = tempDir.resolve("test_log.dat").toFile();
        MiniKafka broker = new MiniKafka(logFile.getAbsolutePath());

        broker.produce("Hello Disk 1");
        broker.produce("Hello Disk 2");

        String msg1 = broker.consume();
        String msg2 = broker.consume();

        assertEquals("Hello Disk 1", msg1);
        assertEquals("Hello Disk 2", msg2);
    }

    @Test
    public void testConcurrentProducersAndConsumers() throws Exception {
        File logFile = tempDir.resolve("concurrent_log.dat").toFile();
        MiniKafka broker = new MiniKafka(logFile.getAbsolutePath());

        int messageCount = 100;
        CountDownLatch finishLine = new CountDownLatch(messageCount);

        Thread consumer = new Thread(() -> {
            try {
                for (int i = 0; i < messageCount; i++) {
                    String msg = broker.consume();
                    assertNotNull(msg);
                    assertTrue(msg.startsWith("Concurrent Message"));
                    finishLine.countDown();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        consumer.start();

        for (int i = 0; i < messageCount; i++) {
            broker.produce("Concurrent Message " + i);
        }

        finishLine.await();
        assertEquals(0, finishLine.getCount());
    }
}