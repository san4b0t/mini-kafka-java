package test.java;

import main.java.MiniKafka;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MiniKafkaTest {

    @Test
    public void testConcurrentProducers() throws InterruptedException {
        MiniKafka broker = new MiniKafka();
        int numberOfThreads = 100;

        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch startingGate = new CountDownLatch(1);
        CountDownLatch finishLine = new CountDownLatch(numberOfThreads);

        for (int i = 0; i < numberOfThreads; ++i) {
            final int threadId = i;
            executor.submit(() -> {
               try {
                   startingGate.await();
                   broker.produce("Test Message from thread " + threadId);
               } catch (InterruptedException e) {
                   Thread.currentThread().interrupt();
               } finally {
                   finishLine.countDown();
               }
            });
        }

        startingGate.countDown();

        finishLine.await();
        executor.shutdown();

        assertDoesNotThrow(() -> {
            String lastMessage = broker.consume(99);
            assertNotNull(lastMessage);
        });
    }
}
