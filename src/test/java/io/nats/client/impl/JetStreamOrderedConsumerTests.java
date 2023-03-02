// Copyright 2020 The NATS Authors
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

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.NatsJetStreamClientError.JsSubOrderedNotAllowOnQueues;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamOrderedConsumerTests extends JetStreamTestBase {

    // ------------------------------------------------------------------------------------------
    // This allows me to intercept messages before it gets to the connection queue
    // which is before the messages is available for nextMessage, or before
    // it gets dispatched to a handler.
    static class OrderedTestDropSimulator extends OrderedManager {
        public OrderedTestDropSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, NatsDispatcher dispatcher) {
            super(conn, js, stream, so, serverCC, queueMode, dispatcher);
        }

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (msg.isJetStream()) {
                long ss = msg.metaData().streamSequence();
                long cs = msg.metaData().consumerSequence();
                if ((ss == 2 && cs == 2) || (ss == 5 && cs == 4)) {
                    return false;
                }
            }

            return super.beforeQueueProcessorImpl(msg);
        }
    }

    // Expected consumer sequence numbers
    static long[] EXPECTED_CON_SEQ_NUMS = new long[] {1, 1, 2, 3, 1, 2};

    @Test
    public void testOrderedConsumerSync() throws Exception {
        runInJsServer(nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            String subject = subject(111);
            createMemoryStream(jsm, stream(111), subject);

            // Get this in place before any subscriptions are made
            ((NatsJetStream)js).PUSH_MESSAGE_MANAGER_FACTORY = OrderedTestDropSimulator::new;

            // The options will be used in various ways
            PushSubscribeOptions pso = PushSubscribeOptions.builder().ordered(true).build();

            // Test queue exception
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, QUEUE, pso));
            assertTrue(iae.getMessage().contains(JsSubOrderedNotAllowOnQueues.id()));

            // Setup sync subscription
            JetStreamSubscription sub = js.subscribe(subject, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server
            sleep(1000);

            // Published messages will be intercepted by the OrderedTestDropSimulator
            jsPublish(js, subject, 101, 6);

            // Loop through the messages to make sure I get stream sequence 1 to 6
            int expectedStreamSeq = 1;
            while (expectedStreamSeq <= 6) {
                Message m = sub.nextMessage(Duration.ofSeconds(1)); // use duration version here for coverage
                if (m != null) {
                    assertEquals(expectedStreamSeq, m.metaData().streamSequence());
                    assertEquals(EXPECTED_CON_SEQ_NUMS[expectedStreamSeq-1], m.metaData().consumerSequence());
                    ++expectedStreamSeq;
                }
            }
        });
    }

    @Test
    public void testOrderedConsumerAsync() throws Exception {
        runInJsServer(nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            String subject = subject(222);
            createMemoryStream(jsm, stream(222), subject);

            // Get this in place before any subscriptions are made
            ((NatsJetStream)js).PUSH_MESSAGE_MANAGER_FACTORY = OrderedTestDropSimulator::new;

            // The options will be used in various ways
            PushSubscribeOptions pso = PushSubscribeOptions.builder().ordered(true).build();

            // We'll need a dispatcher
            Dispatcher d = nc.createDispatcher();

            // Test queue exception
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, QUEUE, d, m -> {}, false, pso));
            assertTrue(iae.getMessage().contains(JsSubOrderedNotAllowOnQueues.id()));

            // Setup async subscription
            CountDownLatch msgLatch = new CountDownLatch(6);
            AtomicInteger received = new AtomicInteger();
            AtomicLong[] ssFlags = new AtomicLong[6];
            AtomicLong[] csFlags = new AtomicLong[6];
            MessageHandler handler = hmsg -> {
                int i = received.incrementAndGet() - 1;
                ssFlags[i] = new AtomicLong(hmsg.metaData().streamSequence());
                csFlags[i] = new AtomicLong(hmsg.metaData().consumerSequence());
                msgLatch.countDown();
            };

            js.subscribe(subject, d, handler, false, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server
            sleep(1000);

            // publish after sub b/c interceptor is set during sub, so before messages come in
            jsPublish(js, subject, 201, 6);

            // wait for the messages
            awaitAndAssert(msgLatch);

            // Loop through the messages to make sure I get stream sequence 1 to 6
            int expectedStreamSeq = 1;
            while (expectedStreamSeq <= 6) {
                int idx = expectedStreamSeq - 1;
                assertEquals(expectedStreamSeq, ssFlags[idx].get());
                assertEquals(EXPECTED_CON_SEQ_NUMS[idx], csFlags[idx].get());
                ++expectedStreamSeq;
            }
        });
    }

    static class OrderedMissHeartbeatSimulator extends OrderedManager {
        public AtomicInteger skip = new AtomicInteger(5);
        public AtomicInteger startups = new AtomicInteger(0);
        public AtomicInteger handles = new AtomicInteger(0);
        public CountDownLatch latch = new CountDownLatch(1);

        public OrderedMissHeartbeatSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, NatsDispatcher dispatcher) {
            super(conn, js, stream, so, serverCC, queueMode, dispatcher);
        }

        @Override
        protected void startup(NatsJetStreamSubscription sub) {
            super.startup(sub);
            startups.incrementAndGet();
        }

        @Override
        protected void handleHeartbeatError() {
            handles.incrementAndGet();
            skip.set(9999); // more than the number of messages left
            super.handleHeartbeatError();
            latch.countDown();
        }

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (skip.decrementAndGet() < 0) {
                return false;
            }
            return super.beforeQueueProcessorImpl(msg);
        }

        public String SID() {
            return sub.getSID();
        }
    }

    @Test
    public void testOrderedConsumerHbSync() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            String subject = subject(333);
            createMemoryStream(jsm, stream(333), subject);

            // Get this in place before any subscriptions are made
            AtomicReference<OrderedMissHeartbeatSimulator> simRef = new AtomicReference<>();
            ((NatsJetStream)js).PUSH_MESSAGE_MANAGER_FACTORY = (conn, lJs, stream, so, serverCC, qmode, dispatcher) -> {
                OrderedMissHeartbeatSimulator sim = new OrderedMissHeartbeatSimulator(conn, lJs, stream, so, serverCC, qmode, dispatcher);
                simRef.set(sim);
                return sim;
            };

            jsPublish(js, subject, 1, 10);

            // Setup subscription
            PushSubscribeOptions pso = PushSubscribeOptions.builder().ordered(true)
                .configuration(ConsumerConfiguration.builder().flowControl(500).build())
                .build();

            JetStreamSubscription sub = js.subscribe(subject, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            OrderedMissHeartbeatSimulator sim = simRef.get();

            String firstSid = null;
            int expectedStreamSeq = 1;
            while (expectedStreamSeq <= 5) {
                Message m = sub.nextMessage(Duration.ofSeconds(1)); // use duration version here for coverage
                if (m != null) {
                    if (firstSid == null) {
                        firstSid = sim.SID();
                    }
                    else {
                        assertEquals(firstSid, sim.SID());
                    }
                    assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                }
            }
            sim.latch.await(10, TimeUnit.SECONDS);

            while (expectedStreamSeq <= 10) {
                Message m = sub.nextMessage(Duration.ofSeconds(1)); // use duration version here for coverage
                if (m != null) {
                    assertNotEquals(firstSid, sim.SID());
                    assertEquals(expectedStreamSeq++, m.metaData().streamSequence());
                }
            }
        });
    }
}
