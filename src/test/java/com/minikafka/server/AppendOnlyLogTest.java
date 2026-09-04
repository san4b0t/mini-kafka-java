package com.minikafka.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AppendOnlyLogTest {

    @TempDir
    Path tempDirectory;

    @Test
    void appendsAndReadsMessagesInOffsetOrder() throws Exception {
        AppendOnlyLog log = new AppendOnlyLog(tempDirectory.resolve("messages.log").toString());

        long firstOffset = log.append("first");
        long secondOffset = log.append("second");

        assertEquals(0, firstOffset);
        assertEquals(recordSize("first"), secondOffset);
        assertEquals("first", log.read(firstOffset));
        assertEquals("second", log.read(secondOffset));
        assertNull(log.read(secondOffset + recordSize("second")));
    }

    @Test
    void calculatesOffsetsFromUtf8ByteLength() throws Exception {
        AppendOnlyLog log = new AppendOnlyLog(tempDirectory.resolve("utf8.log").toString());

        long firstOffset = log.append("你好");
        long secondOffset = log.append("after");

        assertEquals(0, firstOffset);
        assertEquals(recordSize("你好"), secondOffset);
        assertEquals("你好", log.read(firstOffset));
        assertEquals("after", log.read(secondOffset));
    }

    @Test
    void readsDataFromAnExistingLogFile() throws Exception {
        Path logPath = tempDirectory.resolve("persistent.log");
        AppendOnlyLog writer = new AppendOnlyLog(logPath.toString());
        writer.append("persisted");

        AppendOnlyLog reopenedLog = new AppendOnlyLog(logPath.toString());

        assertEquals("persisted", reopenedLog.read(0));
        assertEquals(recordSize("persisted"), reopenedLog.append("next"));
    }

    @Test
    void rejectsAnAppendThatWouldExceedTheLogLimit() throws Exception {
        long maxLogBytes = recordSize("first");
        AppendOnlyLog log = new AppendOnlyLog(
                tempDirectory.resolve("limited.log").toString(), maxLogBytes);
        log.append("first");

        assertThrows(LogCapacityExceededException.class, () -> log.append("next"));
        assertNull(log.read(maxLogBytes));
    }

    private static int recordSize(String message) {
        return Integer.BYTES + message.getBytes(StandardCharsets.UTF_8).length;
    }
}
