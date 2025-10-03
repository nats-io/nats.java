package io.nats.client.impl;

import io.nats.client.Options;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.nats.client.impl.NatsConnectionWriter.END_RECONNECT;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NatsConnectionWriterTests {
    @Test
    public void shouldWriteToDataPort_whenMessageWithEndReconnect() throws IOException, InterruptedException {
        // Given: a linked list (batch) of NatsMessage instances terminated by the END_RECONNECT sentinel
        NatsConnectionWriter writer = new NatsConnectionWriter(new MockNatsConnection(new Options.Builder().build()), null);
        NatsMessage firstNatsMessage = linkedNatsMessageWithEndReconnect();
        CountDownLatch writeToDataPort = new CountDownLatch(1);

        // When: sending the prepared batch through the writer
        writer.sendMessageBatch(firstNatsMessage, new MockDataPort(writeToDataPort), new NatsStatistics());

        // Then: the data port write method should have been invoked within the timeout
        assertTrue(writeToDataPort.await(1, TimeUnit.SECONDS), "DataPort write was not invoked in time. Latch count=" + writeToDataPort.getCount());
    }

    private static NatsMessage linkedNatsMessageWithEndReconnect() {
        NatsMessage firstNatsMessage = new NatsMessage("s", "r", new byte[0]);
        NatsMessage nextNatsMessage = firstNatsMessage;
        for (int i = 0; i < 10; i++) {
            NatsMessage next = new NatsMessage("s", "r", new byte[0]);
            nextNatsMessage.next = next;
            nextNatsMessage = next;
        }
        nextNatsMessage.next = END_RECONNECT;
        return firstNatsMessage;
    }

    private static class MockDataPort implements DataPort {

        private final CountDownLatch writeToDataPort;

        public MockDataPort(CountDownLatch writeToDataPort) {
            this.writeToDataPort = writeToDataPort;
        }

        @Override
        public void connect(@NonNull String serverURI, @NonNull NatsConnection conn, long timeoutNanos) {
            // No-op for test
        }

        @Override
        public void upgradeToSecure() {
            // No-op for test
        }

        @Override
        public int read(byte[] dst, int off, int len) {
            return 0; // Not needed for this test
        }

        @Override
        public void write(byte[] src, int toWrite) {
            writeToDataPort.countDown();
        }

        @Override
        public void shutdownInput() {
            // No-op for test
        }

        @Override
        public void close() {
            // No-op for test
        }

        @Override
        public void flush() {
            // No-op for test
        }
    }
}
