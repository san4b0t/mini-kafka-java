package com.minikafka.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.minikafka.grpc.ConsumeRequest;
import com.minikafka.grpc.ConsumeResponse;
import com.minikafka.grpc.ProduceRequest;
import com.minikafka.grpc.ProduceResponse;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerServerTest {

    @TempDir
    Path tempDirectory;

    private BrokerServer broker;

    @BeforeEach
    void setUp() {
        broker = new BrokerServer(tempDirectory.resolve("broker.log").toString());
    }

    @Test
    void producesAndConsumesMessagesUsingReturnedOffsets() {
        CapturingObserver<ProduceResponse> firstProduce = produce("first");
        CapturingObserver<ProduceResponse> secondProduce = produce("你好");

        assertSuccessful(firstProduce);
        assertSuccessful(secondProduce);
        assertEquals(0, firstProduce.value.getOffset());
        assertEquals(recordSize("first"), secondProduce.value.getOffset());

        CapturingObserver<ConsumeResponse> firstConsume = consume(firstProduce.value.getOffset());
        CapturingObserver<ConsumeResponse> secondConsume = consume(secondProduce.value.getOffset());

        assertSuccessful(firstConsume);
        assertSuccessful(secondConsume);
        assertEquals("first", firstConsume.value.getMessage());
        assertEquals(recordSize("first"), firstConsume.value.getNextOffset());
        assertEquals("你好", secondConsume.value.getMessage());
        assertEquals(recordSize("first") + recordSize("你好"), secondConsume.value.getNextOffset());
    }

    @Test
    void returnsAnEmptyResponseWhenNoMessageExistsAtOffset() {
        CapturingObserver<ConsumeResponse> observer = consume(0);

        assertSuccessful(observer);
        assertEquals("", observer.value.getMessage());
        assertEquals(0, observer.value.getNextOffset());
    }

    @Test
    void rejectsEmptyAndBlankMessages() {
        CapturingObserver<ProduceResponse> empty = produce("");
        CapturingObserver<ProduceResponse> blank = produce("   ");

        assertRejected(empty, Status.Code.INVALID_ARGUMENT);
        assertRejected(blank, Status.Code.INVALID_ARGUMENT);
        assertEquals("", consume(0).value.getMessage());
    }

    @Test
    void rejectsMessagesOverTheConfiguredLimit() {
        broker = new BrokerServer(
                tempDirectory.resolve("message-limited.log").toString(), 100, 5);

        CapturingObserver<ProduceResponse> observer = produce("123456");

        assertRejected(observer, Status.Code.INVALID_ARGUMENT);
        assertEquals("", consume(0).value.getMessage());
    }

    @Test
    void rejectsMessagesWhenTheLogIsFull() {
        long oneRecord = recordSize("first");
        broker = new BrokerServer(
                tempDirectory.resolve("full.log").toString(), oneRecord, 5);

        assertSuccessful(produce("first"));
        CapturingObserver<ProduceResponse> rejected = produce("again");

        assertRejected(rejected, Status.Code.RESOURCE_EXHAUSTED);
        assertEquals("first", consume(0).value.getMessage());
    }

    @Test
    void parsesHumanReadableBinarySizes() {
        assertEquals(1, BrokerServer.parseSizeBytes("1"));
        assertEquals(1024, BrokerServer.parseSizeBytes("1KiB"));
        assertEquals(1024 * 1024, BrokerServer.parseSizeBytes("1MiB"));
        assertEquals(1024L * 1024 * 1024, BrokerServer.parseSizeBytes("1GiB"));
        assertThrows(IllegalArgumentException.class, () -> BrokerServer.parseSizeBytes("0"));
        assertThrows(IllegalArgumentException.class, () -> BrokerServer.parseSizeBytes("1MB"));
    }

    private CapturingObserver<ProduceResponse> produce(String message) {
        CapturingObserver<ProduceResponse> observer = new CapturingObserver<>();
        broker.produce(ProduceRequest.newBuilder().setMessage(message).build(), observer);
        return observer;
    }

    private CapturingObserver<ConsumeResponse> consume(long offset) {
        CapturingObserver<ConsumeResponse> observer = new CapturingObserver<>();
        broker.consume(ConsumeRequest.newBuilder().setOffset(offset).build(), observer);
        return observer;
    }

    private static long recordSize(String message) {
        return Integer.BYTES + message.getBytes(StandardCharsets.UTF_8).length;
    }

    private static <T> void assertSuccessful(CapturingObserver<T> observer) {
        assertNull(observer.error);
        assertTrue(observer.completed);
    }

    private static <T> void assertRejected(
            CapturingObserver<T> observer, Status.Code expectedStatus) {
        assertEquals(expectedStatus, Status.fromThrowable(observer.error).getCode());
        assertFalse(observer.completed);
    }

    private static final class CapturingObserver<T> implements StreamObserver<T> {
        private T value;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable error) {
            this.error = error;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }
}
