// Copyright 2026 The NATS Authors
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

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;
import io.nats.client.Subscription;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

// invalidate() nulls NatsSubscription.incoming from another thread, so any method that reads
// the queue reference twice can see it live on the first read and null on the second. The
// harness below turns that timing problem into a deterministic one: the queue is handed out
// on the first lookup and never again. A method that reads once passes; a method that reads
// twice either throws or reports a value from the second, empty read.
public class NatsConsumerPendingTests {

    @Test
    public void testPendingMessageCountUsesSingleQueueLookup() {
        assertEquals(1, new SingleLookupConsumer(1).getPendingMessageCount());
    }

    @Test
    public void testPendingByteCountUsesSingleQueueLookup() {
        assertTrue(new SingleLookupConsumer(1).getPendingByteCount() > 0);
    }

    @Test
    public void testDeliverabilityStateAvailable() {
        assertEquals(NatsConsumer.DeliverabilityState.AVAILABLE,
            new NoQueueConsumer().getDeliverabilityState(queueWith(1)));
    }

    @Test
    public void testDeliverabilityStateNotAvailableWithNoQueue() {
        assertEquals(NatsConsumer.DeliverabilityState.NOT_AVAILABLE,
            new NoQueueConsumer().getDeliverabilityState(null));
    }

    @Test
    public void testMarkUnsubedForDrainUsesSingleQueueLookup() {
        SingleLookupConsumer consumer = new SingleLookupConsumer(1);
        assertDoesNotThrow(consumer::markUnsubedForDrain);
    }

    // -1 is the contract for "the queue is gone", as distinct from 0 meaning "the queue is
    // there and empty". Callers rely on being able to tell those apart.
    @Test
    public void testNoQueueReportsMinusOne() {
        NoQueueConsumer consumer = new NoQueueConsumer();
        assertEquals(-1, consumer.getPendingMessageCount());
        assertEquals(-1, consumer.getPendingByteCount());
    }

    @Test
    public void testDeliverabilityStateFullOnMessageLimit() {
        NoQueueConsumer consumer = new NoQueueConsumer();
        consumer.setPendingLimits(1, 0);
        assertEquals(NatsConsumer.DeliverabilityState.FULL, consumer.getDeliverabilityState(queueWith(1)));
    }

    @Test
    public void testDeliverabilityStateFullOnByteLimit() {
        NoQueueConsumer consumer = new NoQueueConsumer();
        consumer.setPendingLimits(0, 1);
        assertEquals(NatsConsumer.DeliverabilityState.FULL, consumer.getDeliverabilityState(queueWith(1)));
    }

    @Test
    public void testDeliverabilityStateAvailableWhenUnlimited() {
        NoQueueConsumer consumer = new NoQueueConsumer();
        consumer.setPendingLimits(0, 0);
        assertEquals(NatsConsumer.DeliverabilityState.AVAILABLE, consumer.getDeliverabilityState(queueWith(1)));
    }

    private static ConsumerMessageQueue queueWith(int messages) {
        ConsumerMessageQueue queue = new ConsumerMessageQueue();
        for (int x = 0; x < messages; x++) {
            queue.push(NatsMessage.builder().subject("subject").build());
        }
        return queue;
    }

    // The NOT_AVAILABLE branch of deliverMessage is new behaviour: the message is dropped and
    // counted, but the consumer is NOT marked slow, because a subscription that has gone away
    // is not a slow consumer. Invalidating directly leaves the subscription registered on the
    // connection, so the reader still finds it - the exact state the reported race occurred in.
    @Test
    public void testDeliveryToUnavailableQueueIsDroppedNotMarkedSlow() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
             NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {

            NatsSubscription sub = (NatsSubscription) nc.subscribe("subject");
            sub.invalidate();

            long droppedBefore = sub.getDroppedCount();

            nc.publish("subject", null);
            nc.flush(Duration.ofSeconds(5));

            assertEquals(Connection.Status.CONNECTED, nc.getStatus(), "delivery took the connection down");
            assertEquals(droppedBefore + 1, sub.getDroppedCount(), "message was not counted as dropped");
            assertFalse(sub.isMarkedSlow(), "a gone subscription must not be reported as a slow consumer");
        }
    }


    // nextMessageInternal checks "incoming == null" and then dereferences incoming twice more
    // - once for pop() and once for isRunning(). Nothing here can force an invalidate into
    // those exact gaps, so this pins the contract instead: unsubscribing while another thread
    // is blocked in nextMessage must report IllegalStateException, never a NullPointerException.
    // There was no coverage of this at all before.
    @Test
    public void testUnsubscribeWhileBlockedInNextMessageReportsInactive() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
             NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {

            Subscription sub = nc.subscribe("subject");

            AtomicReference<Throwable> thrown = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);

            Thread waiter = new Thread(() -> {
                started.countDown();
                try {
                    sub.nextMessage(Duration.ofSeconds(10));
                }
                catch (Throwable t) {
                    thrown.set(t);
                }
            });
            waiter.start();

            assertTrue(started.await(5, TimeUnit.SECONDS));
            //noinspection BusyWait
            Thread.sleep(250); // let it get into the blocking pop

            sub.unsubscribe();
            waiter.join(5000);
            assertFalse(waiter.isAlive(), "nextMessage never returned after unsubscribe");

            Throwable t = thrown.get();
            assertNotNull(t, "nextMessage returned quietly instead of reporting the subscription went away");
            assertFalse(t instanceof NullPointerException,
                "unsubscribe raced nextMessage and produced an NPE: " + t);
            assertTrue(t instanceof IllegalStateException, "expected IllegalStateException, got " + t);
            // either message is correct - which one depends on whether the waiter reached the
            // blocking pop before the unsubscribe landed, and that is not something the test
            // can pin down. What matters is that it is this pair and never an NPE.
            assertTrue("This subscription became inactive.".equals(t.getMessage())
                    || "This subscription is inactive.".equals(t.getMessage()),
                "unexpected message: " + t.getMessage());
        }
    }

    // Hands the queue out on the first lookup and null on every lookup after it.
    private static class SingleLookupConsumer extends NatsConsumer {
        private final AtomicInteger lookups = new AtomicInteger();
        private final ConsumerMessageQueue queue;

        SingleLookupConsumer(int messages) {
            super(null);
            queue = new ConsumerMessageQueue();
            for (int x = 0; x < messages; x++) {
                queue.push(NatsMessage.builder().subject("subject").build());
            }
        }

        @Override
        public boolean isActive() {
            return true;
        }

        @Override
        ConsumerMessageQueue getMessageQueue() {
            return lookups.getAndIncrement() == 0 ? queue : null;
        }

        @Override
        void sendUnsubForDrain() {
        }

        @Override
        void cleanUpAfterDrain() {
        }
    }

    // Never has a queue - a consumer that has been invalidated, or a dispatcher subscription.
    private static class NoQueueConsumer extends NatsConsumer {
        NoQueueConsumer() {
            super(null);
        }

        @Override
        public boolean isActive() {
            return false;
        }

        @Override
        ConsumerMessageQueue getMessageQueue() {
            return null;
        }

        @Override
        void sendUnsubForDrain() {
        }

        @Override
        void cleanUpAfterDrain() {
        }
    }
}
