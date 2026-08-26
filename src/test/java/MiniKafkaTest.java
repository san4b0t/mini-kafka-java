package test.java;

import main.java.MiniKafka;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Path;

public class MiniKafkaTest {

    @TempDir
    Path tempDir;

    @Test
    public void testDiskPersistenceAndRead() throws Exception {
        File logFile = tempDir.resolve("test_log.dat").toFile();
        MiniKafka broker = new MiniKafka(logFile.getAbsolutePath());

        // Produce messages (spaces and mutex semaphores are handled internally)
        broker.produce("Hello Disk 1");
        broker.produce("Hello Disk 2");

        // Consume messages in strict FIFO order (no offsets needed anymore)
        String msg1 = broker.consume();
        String msg2 = broker.consume();

        assertEquals("Hello Disk 1", msg1);
        assertEquals("Hello Disk 2", msg2);
    }
}