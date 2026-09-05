package com.minikafka.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class AppendOnlyLogConcurrencyTest {

    private static final int DEFAULT_MESSAGE_COUNT = 5_100;
    private static final int DEFAULT_THREAD_COUNT = 16;
    private static final int MESSAGE_COUNT =
            Integer.getInteger("load.messages", DEFAULT_MESSAGE_COUNT);
    private static final int REQUESTED_THREAD_COUNT =
            Integer.getInteger("load.threads", DEFAULT_THREAD_COUNT);

    @TempDir
    Path tempDirectory;

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void appendsMessagesConcurrentlyAndReportsMetrics() throws Exception {
        assertTrue(MESSAGE_COUNT > 0, "load.messages must be greater than zero");
        assertTrue(REQUESTED_THREAD_COUNT > 0, "load.threads must be greater than zero");

        int threadCount = Math.min(REQUESTED_THREAD_COUNT, MESSAGE_COUNT);
        List<String> messages = createMessages(MESSAGE_COUNT);
        long expectedLogBytes = messages.stream().mapToLong(AppendOnlyLogConcurrencyTest::recordSize).sum();
        Path logPath = tempDirectory.resolve("concurrent-load.log");
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch workersReady = new CountDownLatch(threadCount);
        CountDownLatch startSignal = new CountDownLatch(1);
        List<Future<AppendResult>> futures = new ArrayList<>(MESSAGE_COUNT);

        try (AppendOnlyLog log = new AppendOnlyLog(logPath.toString(), expectedLogBytes)) {
            for (String message : messages) {
                futures.add(executor.submit(() -> {
                    workersReady.countDown();
                    startSignal.await();

                    long startedAt = System.nanoTime();
                    long offset = log.append(message);
                    long latencyNanos = System.nanoTime() - startedAt;
                    return new AppendResult(offset, message, latencyNanos);
                }));
            }

            assertTrue(workersReady.await(10, TimeUnit.SECONDS),
                    "Worker threads did not become ready in time");
            long loadStartedAt = System.nanoTime();
            startSignal.countDown();

            List<AppendResult> results = new ArrayList<>(MESSAGE_COUNT);
            for (Future<AppendResult> future : futures) {
                results.add(future.get(90, TimeUnit.SECONDS));
            }
            long elapsedNanos = System.nanoTime() - loadStartedAt;

            Map<Long, String> messagesByOffset = new HashMap<>();
            long[] latenciesNanos = new long[MESSAGE_COUNT];
            for (int i = 0; i < results.size(); i++) {
                AppendResult result = results.get(i);
                assertNull(messagesByOffset.put(result.offset(), result.message()),
                        "Two appends returned the same offset " + result.offset());
                latenciesNanos[i] = result.latencyNanos();
            }

            assertEquals(MESSAGE_COUNT, messagesByOffset.size());
            assertEquals(expectedLogBytes, Files.size(logPath));
            for (Map.Entry<Long, String> entry : messagesByOffset.entrySet()) {
                assertEquals(entry.getValue(), log.read(entry.getKey()),
                        "Message was corrupted at offset " + entry.getKey());
            }

            String report = createReport(
                    MESSAGE_COUNT,
                    threadCount,
                    expectedLogBytes,
                    elapsedNanos,
                    latenciesNanos);
            System.out.println(report);
            writeReport(report);
        } finally {
            startSignal.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                    "Load-test executor did not terminate");
        }
    }

    private static List<String> createMessages(int count) {
        List<String> messages = new ArrayList<>(count);
        String padding = "x".repeat(100);
        for (int i = 0; i < count; i++) {
            messages.add("message-%05d-%s".formatted(i, padding));
        }
        return messages;
    }

    private static long recordSize(String message) {
        return Integer.BYTES + message.getBytes(StandardCharsets.UTF_8).length;
    }

    private static String createReport(
            int messageCount,
            int threadCount,
            long totalBytes,
            long elapsedNanos,
            long[] latenciesNanos) {
        Arrays.sort(latenciesNanos);
        double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        double throughput = messageCount / elapsedSeconds;
        double mebibytesPerSecond = totalBytes / (1024.0 * 1024.0) / elapsedSeconds;
        double averageLatencyMillis = Arrays.stream(latenciesNanos).average().orElse(0) / 1_000_000.0;

        return String.format(Locale.ROOT, """
                Append-only log concurrency metrics
                messages=%d
                threads=%d
                errors=0
                unique_offsets=%d
                total_bytes=%d
                elapsed_ms=%.3f
                throughput_messages_per_second=%.1f
                throughput_mib_per_second=%.3f
                latency_average_ms=%.3f
                latency_p50_ms=%.3f
                latency_p95_ms=%.3f
                latency_p99_ms=%.3f
                latency_max_ms=%.3f
                """,
                messageCount,
                threadCount,
                messageCount,
                totalBytes,
                elapsedNanos / 1_000_000.0,
                throughput,
                mebibytesPerSecond,
                averageLatencyMillis,
                percentileMillis(latenciesNanos, 0.50),
                percentileMillis(latenciesNanos, 0.95),
                percentileMillis(latenciesNanos, 0.99),
                latenciesNanos[latenciesNanos.length - 1] / 1_000_000.0);
    }

    private static double percentileMillis(long[] sortedLatenciesNanos, double percentile) {
        int index = (int) Math.ceil(percentile * sortedLatenciesNanos.length) - 1;
        return sortedLatenciesNanos[index] / 1_000_000.0;
    }

    private static void writeReport(String report) throws Exception {
        Path metricsDirectory = Path.of("target", "metrics");
        Files.createDirectories(metricsDirectory);
        Files.writeString(metricsDirectory.resolve("append-only-log-load.txt"), report);
    }

    private record AppendResult(long offset, String message, long latencyNanos) {
    }
}
