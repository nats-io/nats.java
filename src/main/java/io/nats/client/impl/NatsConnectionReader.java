package io.nats.client.impl;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface NatsConnectionReader {
    void init() throws InterruptedException, ExecutionException;

    void readNow() throws IOException;
}
