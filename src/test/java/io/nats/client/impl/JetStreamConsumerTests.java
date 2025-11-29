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
import io.nats.client.utils.VersionUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.NatsJetStreamClientError.JsSubOrderedNotAllowOnQueues;
import static io.nats.client.utils.ThreadUtils.sleep;
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
        runInShared((nc, ctx) -> {
            // Get this in place before any subscriptions are made
            ctx.js._pushOrderedMessageManagerFactory = OrderedTestDropSimulator::new;

            // Test queue exception
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> ctx.js.subscribe(ctx.subject(), random(), PushSubscribeOptions.builder().ordered(true).build()));
            assertTrue(iae.getMessage().contains(JsSubOrderedNotAllowOnQueues.id()));

            // Setup sync subscription
            _testOrderedConsumerSync(ctx, null, PushSubscribeOptions.builder().ordered(true).build());

            _testOrderedConsumerSync(ctx, ctx.consumerName(), PushSubscribeOptions.builder().name(ctx.consumerName()).ordered(true).build());
        });
    }

    private static void _testOrderedConsumerSync(JetStreamTestingContext ctx, String consumerNamePrefix, PushSubscribeOptions pso) throws IOException, JetStreamApiException, InterruptedException {
        JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), pso);
        String firstConsumerName = validateOrderedConsumerNamePrefix(sub, consumerNamePrefix);

        // Published messages will be intercepted by the OrderedTestDropSimulator
        jsPublish(ctx.js, ctx.subject(), 101, 6);

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
        reValidateOrderedConsumerNamePrefix(sub, consumerNamePrefix, firstConsumerName);
    }

    private static String validateOrderedConsumerNamePrefix(JetStreamSubscription sub, String consumerNamePrefix) throws IOException, JetStreamApiException {
        String firstConsumerName = sub.getConsumerName();
        if (consumerNamePrefix != null) {
            assertEquals(firstConsumerName, sub.getConsumerInfo().getName());
            assertNotEquals(consumerNamePrefix, firstConsumerName);
            assertTrue(firstConsumerName.startsWith(consumerNamePrefix));
        }
        return firstConsumerName;
    }

    private static void reValidateOrderedConsumerNamePrefix(JetStreamSubscription sub, String consumerNamePrefix, String firstConsumerName) throws IOException, JetStreamApiException {
        if (consumerNamePrefix != null) {
            String currentConsumerName = sub.getConsumerName();
            assertEquals(currentConsumerName, sub.getConsumerInfo().getName());
            assertNotEquals(firstConsumerName, currentConsumerName);
            assertTrue(currentConsumerName.startsWith(consumerNamePrefix));
        }
    }

    @Test
    public void testOrderedConsumerAsyncNoName() throws Exception {
        runInShared((nc, ctx) -> {
            // without name (prefix)
            _testOrderedConsumerAsync(nc, ctx, null,
                PushSubscribeOptions.builder().ordered(true).build());
        });
    }

    @Test
    public void testOrderedConsumerAsyncWithName() throws Exception {
        runInShared((nc, ctx) -> {
            // with name (prefix)
            _testOrderedConsumerAsync(nc, ctx, ctx.consumerName(),
                PushSubscribeOptions.builder().name(ctx.consumerName()).ordered(true).build());
        });
    }

    private static void _testOrderedConsumerAsync(Connection nc, JetStreamTestingContext ctx, String consumerNamePrefix, PushSubscribeOptions pso) throws JetStreamApiException, IOException, InterruptedException {
        // Get this in place before any subscriptions are made
        ctx.js._pushOrderedMessageManagerFactory = OrderedTestDropSimulator::new;

        // We'll need a dispatcher
        Dispatcher d = nc.createDispatcher();

        // Test queue exception
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> ctx.js.subscribe(ctx.subject(), random(), d, m -> {}, false, pso));
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

        JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), d, handler, false, pso);
        String firstConsumerName = validateOrderedConsumerNamePrefix(sub, consumerNamePrefix);

        // publish after sub b/c interceptor is set during sub, so before messages come in
        jsPublish(ctx.js, ctx.subject(), 201, 6);

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

        reValidateOrderedConsumerNamePrefix(sub, consumerNamePrefix, firstConsumerName);
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
        ListenerForTesting listener = new ListenerForTesting();
        runInSharedOwnNc(listener, (nc, ctx) -> {
            Dispatcher d = nc.createDispatcher();
            ConsumerConfiguration cc = ConsumerConfiguration.builder().idleHeartbeat(100).build();
            JetStream js = ctx.js;
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            SimulatorState state = setupFactory(js);
            JetStreamSubscription sub = js.subscribe(ctx.subject(), pso);
            validate(sub, listener, state, null);

            state = setupFactory(js);
            sub = js.subscribe(ctx.subject(), d, m -> {}, false, pso);
            validate(sub, listener, state, d);

            pso = PushSubscribeOptions.builder().ordered(true).configuration(cc).build();
            state = setupOrderedFactory(js);
            sub = js.subscribe(ctx.subject(), pso);
            validate(sub, listener, state, null);

            state = setupOrderedFactory(js);
            sub = js.subscribe(ctx.subject(), d, m -> {}, false, pso);
            validate(sub, listener, state, d);

            state = setupPullFactory(js);
            sub = js.subscribe(ctx.subject(), PullSubscribeOptions.DEFAULT_PULL_OPTS);
            sub.pull(PullRequestOptions.builder(1).idleHeartbeat(100).expiresIn(2000).build());
            validate(sub, listener, state, null);
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
        ((NatsJetStream)js)._pullMessageManagerFactory =
            (conn, lJs, stream, so, serverCC, qmode, dispatcher) ->
                new PullHeartbeatErrorSimulator(conn, false, state);
        return state;
    }

    @Test
    public void testMultipleSubjectFilters() throws Exception {
        runInSharedCustom(VersionUtils::atLeast2_10, (nc, ctx) -> {
            ctx.createOrReplaceStream(2);
            jsPublish(ctx.js, ctx.subject(0), 10);
            jsPublish(ctx.js, ctx.subject(1), 5);

            // push ephemeral
            ConsumerConfiguration cc = ConsumerConfiguration.builder().filterSubjects(ctx.subject(0), ctx.subject(1)).build();
            JetStreamSubscription sub = ctx.js.subscribe(null, PushSubscribeOptions.builder().configuration(cc).build());
            validateMultipleSubjectFilterSub(sub, ctx.subject(0));

            // pull ephemeral
            sub = ctx.js.subscribe(null, PullSubscribeOptions.builder().configuration(cc).build());
            sub.pullExpiresIn(15, 1000);
            validateMultipleSubjectFilterSub(sub, ctx.subject(0));

            // push named
            String name = random();
            cc = ConsumerConfiguration.builder().filterSubjects(ctx.subject(0), ctx.subject(1)).name(name).deliverSubject(random()).build();
            ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);
            sub = ctx.js.subscribe(null, PushSubscribeOptions.builder().configuration(cc).build());
            validateMultipleSubjectFilterSub(sub, ctx.subject(0));

            name = random();
            cc = ConsumerConfiguration.builder().filterSubjects(ctx.subject(0), ctx.subject(1)).name(name).deliverSubject(random()).build();
            ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);
            sub = ctx.js.subscribe(null, PushSubscribeOptions.bind(ctx.stream, name));
            validateMultipleSubjectFilterSub(sub, ctx.subject(0));

            // pull named
            name = random();
            cc = ConsumerConfiguration.builder().filterSubjects(ctx.subject(0), ctx.subject(1)).name(name).build();
            ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);
            sub = ctx.js.subscribe(null, PullSubscribeOptions.builder().configuration(cc).build());
            sub.pullExpiresIn(15, 1000);
            validateMultipleSubjectFilterSub(sub, ctx.subject(0));

            name = random();
            cc = ConsumerConfiguration.builder().filterSubjects(ctx.subject(0), ctx.subject(1)).name(name).build();
            ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);
            sub = ctx.js.subscribe(null, PullSubscribeOptions.bind(ctx.stream, name));
            sub.pullExpiresIn(15, 1000);
            validateMultipleSubjectFilterSub(sub, ctx.subject(0));
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

    @Test
    public void testRaiseStatusWarnings1194() throws Exception {
        ListenerForTesting listener = new ListenerForTesting(false, false);
        runInSharedOwnNc(listener, (nc, ctx) -> {
            // Setup
            StreamContext streamContext = nc.getStreamContext(ctx.stream);

            // Setting maxBatch=1, so we shouldn't allow fetching more messages at once.
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder().filterSubject(ctx.subject()).maxBatch(1).build();
            ConsumerContext consumerContext = streamContext.createOrUpdateConsumer(consumerConfig);

            int count = 0;

            // Fetching a batch of 100 messages is not allowed, so we rightfully don't get any messages and wait for timeout.
            // But we don't get informed about the status message.
            FetchConsumeOptions fco = FetchConsumeOptions.builder()
                .maxMessages(100)
                .expiresIn(1000)
                .build();
            try (FetchConsumer fetchConsumer = consumerContext.fetch(fco)) {
                Message msg;
                while ((msg = fetchConsumer.nextMessage()) != null) {
                    msg.ack();
                    count++;
                }
            }
            assertEquals(0, count);
            assertEquals(0, listener.getPullStatusWarnings().size());

            fco = FetchConsumeOptions.builder()
                .maxMessages(100)
                .expiresIn(1000)
                .raiseStatusWarnings()
                .build();
            try (FetchConsumer fetchConsumer = consumerContext.fetch(fco)) {
                Message msg;
                while ((msg = fetchConsumer.nextMessage()) != null) {
                    msg.ack();
                    count++;
                }
            }
            assertEquals(0, count);
            assertEquals(1, listener.getPullStatusWarnings().size());
        });
    }
}
