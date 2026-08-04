// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;

import io.nats.client.Message;
import io.nats.client.Options;
import io.nats.client.ReadListener;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

// Covers NatsConnectionReader.setConnection repointing the reader's connection, and the ReadListener
// being re-derived from the new connection's Options - rebuilding only when the configured listener
// actually changed. Uses unconnected connections: the reader and its callback executor exist after
// construction, so no server is needed.
public class NatsConnectionReaderRepointTests {

    static class RecordingReadListener implements ReadListener {
        final AtomicInteger protocolCount = new AtomicInteger();
        final AtomicInteger messageCount = new AtomicInteger();
        volatile CountDownLatch latch;

        @Override
        public void protocol(String op, String text) {
            protocolCount.incrementAndGet();
            CountDownLatch l = latch;
            if (l != null) { l.countDown(); }
        }

        @Override
        public void message(String op, Message message) {
            messageCount.incrementAndGet();
            CountDownLatch l = latch;
            if (l != null) { l.countDown(); }
        }
    }

    private static NatsConnection unconnected(ReadListener rl) {
        Options.Builder b = new Options.Builder();
        if (rl != null) {
            b.readListener(rl);
        }
        return new NatsConnection(b.build());
    }

    private static void closeQuietly(NatsConnection... conns) {
        for (NatsConnection c : conns) {
            try { c.close(); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    @Test
    public void repoint_keepsListenerWhenUnchanged_rebuildsWhenChanged() throws Exception {
        RecordingReadListener rlA = new RecordingReadListener();
        RecordingReadListener rlB = new RecordingReadListener();

        NatsConnection connA = unconnected(rlA);       // has listener rlA
        NatsConnection connASameRl = unconnected(rlA); // different connection, SAME listener instance
        NatsConnection connB = unconnected(rlB);       // different listener
        NatsConnection connNull1 = unconnected(null);  // no listener
        NatsConnection connNull2 = unconnected(null);  // no listener
        try {
            NatsConnectionReader reader = connA.reader;

            ReadListener wrapperForA = reader.readListenerForTesting();
            assertNotNull(wrapperForA);

            // Same underlying listener instance -> the current wrapper is still correct, keep it.
            // (This is the "readListener != null && newSuppliedUserRl == currentUserRl" keep path -
            //  proof it is reachable / not "never true".)
            reader.setConnection(connASameRl);
            assertSame(wrapperForA, reader.readListenerForTesting());

            // Different listener -> rebuild.
            reader.setConnection(connB);
            ReadListener wrapperForB = reader.readListenerForTesting();
            assertNotNull(wrapperForB);
            assertNotSame(wrapperForA, wrapperForB);

            // No listener -> rebuild to the no-op.
            reader.setConnection(connNull1);
            ReadListener noop = reader.readListenerForTesting();
            assertNotNull(noop);
            assertNotSame(wrapperForB, noop);

            // Still no listener -> keep the no-op (null -> null keep path).
            reader.setConnection(connNull2);
            assertSame(noop, reader.readListenerForTesting());
        }
        finally {
            closeQuietly(connA, connASameRl, connB, connNull1, connNull2);
        }
    }

    @Test
    public void repoint_dispatchesToTheNewConnectionsListener() throws Exception {
        RecordingReadListener rlA = new RecordingReadListener();
        RecordingReadListener rlB = new RecordingReadListener();

        NatsConnection connA = unconnected(rlA);
        NatsConnection connB = unconnected(rlB);
        try {
            NatsConnectionReader reader = connA.reader;

            // Before repoint: the reader's listener dispatches to connA's listener (rlA).
            rlA.latch = new CountDownLatch(1);
            reader.readListenerForTesting().protocol("PONG", null);
            assertTrue(rlA.latch.await(2, TimeUnit.SECONDS), "rlA should have been called");
            assertEquals(1, rlA.protocolCount.get());
            assertEquals(0, rlB.protocolCount.get());

            // After repoint to connB: dispatch follows to connB's listener (rlB), not rlA.
            reader.setConnection(connB);
            rlB.latch = new CountDownLatch(1);
            reader.readListenerForTesting().message("MSG", null);
            assertTrue(rlB.latch.await(2, TimeUnit.SECONDS), "rlB should have been called");
            assertEquals(1, rlB.messageCount.get());
            assertEquals(0, rlA.messageCount.get());
        }
        finally {
            closeQuietly(connA, connB);
        }
    }
}
