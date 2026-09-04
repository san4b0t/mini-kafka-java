package com.minikafka.server;

import java.io.IOException;

public class LogCapacityExceededException extends IOException {

    public LogCapacityExceededException(long maxLogBytes, long currentLogBytes, long requestedBytes) {
        super("Log capacity exceeded: maximum=" + maxLogBytes
                + " bytes, current=" + currentLogBytes
                + " bytes, requested=" + requestedBytes + " bytes");
    }
}
