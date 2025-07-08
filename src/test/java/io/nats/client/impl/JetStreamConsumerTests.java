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
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.NatsJetStreamClientError.JsSubOrderedNotAllowOnQueues;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamConsumerTests extends JetStreamTestBase {

    // ------------------------------------------------------------------------------------------
    // This allows me to intercept messages before it gets to the connection queue,
    // which is before the messages are available for "nextMessage",
    // or before it gets dispatched to a handler.
    static class OrderedTestDropSimulator extends OrderedMessageManager {
        public OrderedTestDropSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode) {
            super(conn, js, stream, so, serverCC, queueMode, syncMode);
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
        jsServer.run(nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            // Get this in place before any subscriptions are made
            ((NatsJetStream)js)._pushOrderedMessageManagerFactory = OrderedTestDropSimulator::new;

            // Test queue exception
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> js.subscribe(tsc.subject(), QUEUE, PushSubscribeOptions.builder().ordered(true).build()));
            assertTrue(iae.getMessage().contains(JsSubOrderedNotAllowOnQueues.id()));

            // Setup sync subscription
            _testOrderedConsumerSync(js, tsc, null, PushSubscribeOptions.builder().ordered(true).build());

            String consumerName = "prefix"; // prefix();
            _testOrderedConsumerSync(js, tsc, consumerName, PushSubscribeOptions.builder().name(consumerName).ordered(true).build());
        });
    }

    private static void _testOrderedConsumerSync(JetStream js, TestingStreamContainer tsc, String consumerNamePrefix, PushSubscribeOptions pso) throws IOException, JetStreamApiException, TimeoutException, InterruptedException {
        JetStreamSubscription sub = js.subscribe(tsc.subject(), pso);
        String firstConsumerName = checkPrefix(sub, consumerNamePrefix);

        // Published messages will be intercepted by the OrderedTestDropSimulator
        jsPublish(js, tsc.subject(), 101, 6);

        // Loop through the messages to make sure I get stream sequence 1 to 6
        int expectedStreamSeq = 1;
        while (expectedStreamSeq <= 6) {
            Message m = sub.nextMessage(Duration.ofSeconds(1)); // use the duration version here for coverage
            if (m != null) {
                assertEquals(expectedStreamSeq, m.metaData().streamSequence());
                assertEquals(EXPECTED_CON_SEQ_NUMS[expectedStreamSeq-1], m.metaData().consumerSequence());
                ++expectedStreamSeq;
            }
        }
        reCheckPrefix(sub, consumerNamePrefix, firstConsumerName);
    }

    private static String checkPrefix(JetStreamSubscription sub, String consumerNamePrefix) throws IOException, JetStreamApiException {
        String firstConsumerName = null;
        if (consumerNamePrefix != null) {
            firstConsumerName = sub.getConsumerName();
            assertEquals(firstConsumerName, sub.getConsumerInfo().getName());
            assertNotEquals(consumerNamePrefix, firstConsumerName);
            assertTrue(firstConsumerName.startsWith(consumerNamePrefix));
        }
        return firstConsumerName;
    }

    private static void reCheckPrefix(JetStreamSubscription sub, String consumerNamePrefix, String firstConsumerName) throws IOException, JetStreamApiException {
        if (consumerNamePrefix != null) {
            String currentConsumerName = sub.getConsumerName();
            assertEquals(currentConsumerName, sub.getConsumerInfo().getName());
            assertNotEquals(firstConsumerName, currentConsumerName);
            assertTrue(currentConsumerName.startsWith(consumerNamePrefix));
        }
    }

    @Test
    public void testOrderedConsumerAsync() throws Exception {
        jsServer.run(nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();
            _testOrderedConsumerAsync(nc, jsm, js, null, PushSubscribeOptions.builder().ordered(true).build());
            String customName = variant();
            _testOrderedConsumerAsync(nc, jsm, js, customName, PushSubscribeOptions.builder().name(customName).ordered(true).build());
        });
    }

    private static void _testOrderedConsumerAsync(Connection nc, JetStreamManagement jsm, JetStream js, String consumerNamePrefix, PushSubscribeOptions pso) throws JetStreamApiException, IOException, TimeoutException, InterruptedException {
        TestingStreamContainer tsc = new TestingStreamContainer(jsm);

        // Get this in place before any subscriptions are made
        ((NatsJetStream) js)._pushOrderedMessageManagerFactory = OrderedTestDropSimulator::new;

        // We'll need a dispatcher
        Dispatcher d = nc.createDispatcher();

        // Test queue exception
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), QUEUE, d, m -> {}, false, pso));
        assertTrue(iae.getMessage().contains(JsSubOrderedNotAllowOnQueues.id()));

        // Set up an async subscription
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

        JetStreamSubscription sub = js.subscribe(tsc.subject(), d, handler, false, pso);
        String firstConsumerName = checkPrefix(sub, consumerNamePrefix);

        // publish after sub b/c interceptor is set during sub, so before messages come in
        jsPublish(js, tsc.subject(), 201, 6);

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

        reCheckPrefix(sub, consumerNamePrefix, firstConsumerName);
    }

    static class SimulatorState {
        public final CountDownLatch latch = new CountDownLatch(1);
        public final AtomicInteger hbCounter = new AtomicInteger();
    }

    static class HeartbeatErrorSimulator extends PushMessageManager {
        final SimulatorState state;

        public HeartbeatErrorSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode,
                                       SimulatorState state) {
            super(conn, js, stream, so, serverCC, queueMode, syncMode);
            this.state = state;
        }

        @Override
        protected void handleHeartbeatError() {
            super.handleHeartbeatError();
            state.latch.countDown();
        }

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (msg.isStatusMessage() && msg.getStatus().isHeartbeat()) {
                state.hbCounter.incrementAndGet();
            }
            return false;
        }
    }

    static class OrderedHeartbeatErrorSimulator extends OrderedMessageManager {
        final SimulatorState state;

        public OrderedHeartbeatErrorSimulator(NatsConnection conn, NatsJetStream js, String stream, SubscribeOptions so, ConsumerConfiguration serverCC, boolean queueMode, boolean syncMode,
                                              SimulatorState state) {
            super(conn, js, stream, so, serverCC, queueMode, syncMode);
            this.state = state;
        }

        @Override
        protected void handleHeartbeatError() {
            super.handleHeartbeatError();
            state.latch.countDown();
        }

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (msg.isStatusMessage() && msg.getStatus().isHeartbeat()) {
                state.hbCounter.incrementAndGet();
            }
            return false;
        }
    }

    static class PullHeartbeatErrorSimulator extends PullMessageManager {
        public final SimulatorState state;

        public PullHeartbeatErrorSimulator(NatsConnection conn, boolean syncMode, SimulatorState state) {
            super(conn, PullSubscribeOptions.DEFAULT_PULL_OPTS, syncMode);
            this.state = state;
        }

        @Override
        protected void handleHeartbeatError() {
            super.handleHeartbeatError();
            state.latch.countDown();
        }

        @Override
        protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
            if (msg.isStatusMessage() && msg.getStatus().isHeartbeat()) {
                state.hbCounter.incrementAndGet();
            }
            return false;
        }
    }

    @Test
    public void testHeartbeatError() throws Exception {
        ListenerForTesting listenerForTesting = new ListenerForTesting();
        runInJsServer(listenerForTesting, nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            JetStream js = nc.jetStream();

            Dispatcher d = nc.createDispatcher();
            ConsumerConfiguration cc = ConsumerConfiguration.builder().idleHeartbeat(100).build();

            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            SimulatorState state = setupFactory(js);
            JetStreamSubscription sub = js.subscribe(tsc.subject(), pso);
            validate(sub, listenerForTesting, state, null);

            state = setupFactory(js);
            sub = js.subscribe(tsc.subject(), d, m -> {}, false, pso);
            validate(sub, listenerForTesting, state, d);

            pso = PushSubscribeOptions.builder().ordered(true).configuration(cc).build();
            state = setupOrderedFactory(js);
            sub = js.subscribe(tsc.subject(), pso);
            validate(sub, listenerForTesting, state, null);

            state = setupOrderedFactory(js);
            sub = js.subscribe(tsc.subject(), d, m -> {}, false, pso);
            validate(sub, listenerForTesting, state, d);

            state = setupPullFactory(js);
            sub = js.subscribe(tsc.subject(), PullSubscribeOptions.DEFAULT_PULL_OPTS);
            sub.pull(PullRequestOptions.builder(1).idleHeartbeat(100).expiresIn(2000).build());
            validate(sub, listenerForTesting, state, null);
        });
    }

    private static void validate(JetStreamSubscription sub, ListenerForTesting listener, SimulatorState state, Dispatcher d) throws InterruptedException {
        //noinspection ResultOfMethodCallIgnored
        state.latch.await(2, TimeUnit.SECONDS);
        if (d == null) {
            sub.unsubscribe();
        }
        else {
            d.unsubscribe(sub);
        }
        assertEquals(0, state.latch.getCount());
        assertTrue(state.hbCounter.get() > 0);
        boolean gotHbAlarm = false;
        for (int x = 0; x < 50; x++) {
            gotHbAlarm = !listener.getHeartbeatAlarms().isEmpty();
            if (gotHbAlarm) {
                break;
            }
            sleep(10);
        }
        assertTrue(gotHbAlarm);
        listener.reset();
    }

    private static SimulatorState setupFactory(JetStream js) {
        SimulatorState state = new SimulatorState();
        ((NatsJetStream)js)._pushMessageManagerFactory =
            (conn, lJs, stream, so, serverCC, qmode, dispatcher) ->
                new HeartbeatErrorSimulator(conn, lJs, stream, so, serverCC, qmode, dispatcher, state);
        return state;
    }

    private static SimulatorState setupOrderedFactory(JetStream js) {
        SimulatorState state = new SimulatorState();
        ((NatsJetStream)js)._pushOrderedMessageManagerFactory =
            (conn, lJs, stream, so, serverCC, qmode, dispatcher) ->
                new OrderedHeartbeatErrorSimulator(conn, lJs, stream, so, serverCC, qmode, dispatcher, state);
        return state;
    }

    private static SimulatorState setupPullFactory(JetStream js) {
        SimulatorState state = new SimulatorState();
        // the expected latch count is 1 b/c pull is dead once there is a hb error
        CountDownLatch latch = new CountDownLatch(1);
        ((NatsJetStream)js)._pullMessageManagerFactory =
            (conn, lJs, stream, so, serverCC, qmode, dispatcher) ->
                new PullHeartbeatErrorSimulator(conn, false, state);
        return state;
    }

    @Test
    public void testMultipleSubjectFilters() throws Exception {
        jsServer.run(TestBase::atLeast2_10, nc -> {
            // Setup
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(nc, 2);

            jsPublish(js, tsc.subject(0), 10);
            jsPublish(js, tsc.subject(1), 5);

            // push ephemeral
            ConsumerConfiguration cc = ConsumerConfiguration.builder().filterSubjects(tsc.subject(0), tsc.subject(1)).build();
            JetStreamSubscription sub = js.subscribe(null, PushSubscribeOptions.builder().configuration(cc).build());
            validateMultipleSubjectFilterSub(sub, tsc.subject(0));

            // pull ephemeral
            sub = js.subscribe(null, PullSubscribeOptions.builder().configuration(cc).build());
            sub.pullExpiresIn(15, 1000);
            validateMultipleSubjectFilterSub(sub, tsc.subject(0));

            // push named
            String name = name();
            cc = ConsumerConfiguration.builder().filterSubjects(tsc.subject(0), tsc.subject(1)).name(name).deliverSubject(deliver()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);
            sub = js.subscribe(null, PushSubscribeOptions.builder().configuration(cc).build());
            validateMultipleSubjectFilterSub(sub, tsc.subject(0));

            name = name();
            cc = ConsumerConfiguration.builder().filterSubjects(tsc.subject(0), tsc.subject(1)).name(name).deliverSubject(deliver()).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);
            sub = js.subscribe(null, PushSubscribeOptions.bind(tsc.stream, name));
            validateMultipleSubjectFilterSub(sub, tsc.subject(0));

            // pull named
            name = name();
            cc = ConsumerConfiguration.builder().filterSubjects(tsc.subject(0), tsc.subject(1)).name(name).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);
            sub = js.subscribe(null, PullSubscribeOptions.builder().configuration(cc).build());
            sub.pullExpiresIn(15, 1000);
            validateMultipleSubjectFilterSub(sub, tsc.subject(0));

            name = name();
            cc = ConsumerConfiguration.builder().filterSubjects(tsc.subject(0), tsc.subject(1)).name(name).build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);
            sub = js.subscribe(null, PullSubscribeOptions.bind(tsc.stream, name));
            sub.pullExpiresIn(15, 1000);
            validateMultipleSubjectFilterSub(sub, tsc.subject(0));
        });
    }

    private static void validateMultipleSubjectFilterSub(JetStreamSubscription sub, String subject0) throws InterruptedException {
        int count1 = 0;
        int count2 = 0;
        Message m = sub.nextMessage(1000);
        while (m != null) {
            if (m.getSubject().equals(subject0)) {
                count1++;
            }
            else {
                count2++;
            }
            m = sub.nextMessage(1000);
        }

        assertEquals(10, count1);
        assertEquals(5, count2);
    }

}
