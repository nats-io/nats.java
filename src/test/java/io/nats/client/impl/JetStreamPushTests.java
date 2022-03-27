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
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.PublishAck;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.NatsJetStreamClientError.JsSubOrderedNotAllowOnQueues;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPushTests extends JetStreamTestBase {

    @Test
    public void testPushEphemeralNullDeliver() throws Exception {
        _testPushEphemeral(null);
    }

    @Test
    public void testPushEphemeralWithDeliver() throws Exception {
        _testPushEphemeral(DELIVER);
    }

    private void _testPushEphemeral(String deliverSubject) throws Exception {
        runInJsServer(nc -> {
            // create the stream.
            createDefaultTestStream(nc);

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // publish some messages
            jsPublish(js, SUBJECT, 1, 5);

            // Build our subscription options.
            PushSubscribeOptions options = PushSubscribeOptions.builder().deliverSubject(deliverSubject).build();

            // Subscription 1
            JetStreamSubscription sub1 = js.subscribe(SUBJECT, options);
            assertSubscription(sub1, STREAM, null, deliverSubject, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available
            List<Message> messages1 = readMessagesAck(sub1);
            int total = messages1.size();
            validateRedAndTotal(5, messages1.size(), 5, total);

            // read again, nothing should be there
            List<Message> messages0 = readMessagesAck(sub1);
            total += messages0.size();
            validateRedAndTotal(0, messages0.size(), 5, total);

            sub1.unsubscribe(); // needed for deliver subject version b/c the sub
                                // would be identical. without ds, the ds is generated each
                                // time so is unique

            // Subscription 2
            JetStreamSubscription sub2 = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available, same messages
            List<Message> messages2 = readMessagesAck(sub2);
            total = messages2.size();
            validateRedAndTotal(5, messages2.size(), 5, total);

            // read again, nothing should be there
            messages0 = readMessagesAck(sub2);
            total += messages0.size();
            validateRedAndTotal(0, messages0.size(), 5, total);

            assertSameMessages(messages1, messages2);

            sub2.unsubscribe();

            // Subscription 3 testing null timeout
            JetStreamSubscription sub3 = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server
            sleep(1000); // give time to make sure the messages get to the client

            messages0 = readMessagesAck(sub3, null);
            validateRedAndTotal(5, messages0.size(), 5, 5);

            // Subscription 4 testing timeout <= 0 duration / millis
            JetStreamSubscription sub4 = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server
            sleep(1000); // give time to make sure the messages get to the client
            assertNotNull(sub4.nextMessage(Duration.ZERO));
            assertNotNull(sub4.nextMessage(-1));

            // get the rest
            messages0 = readMessagesAck(sub4, null);
            validateRedAndTotal(3, messages0.size(), 3, 3);
        });
    }

    @Test
    public void testPushDurableNullDeliver() throws Exception {
        _testPushDurable(null);
    }

    @Test
    public void testPushDurableWithDeliver() throws Exception {
        _testPushDurable(DELIVER);
    }

    private void _testPushDurable(String deliverSubject) throws Exception {
        runInJsServer(nc -> {
            // create the stream.
            createDefaultTestStream(nc);

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // For async, create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            // Build our subscription options normally
            PushSubscribeOptions options1 = PushSubscribeOptions.builder()
                .durable(DURABLE)
                .deliverSubject(deliverSubject)
                .build();
            _testPushDurableSubSync(deliverSubject, nc, js, () -> js.subscribe(SUBJECT, options1));
            _testPushDurableSubAsync(js, dispatcher, (d, h) -> js.subscribe(SUBJECT, d, h, false, options1));

            // bind long form
            PushSubscribeOptions options2 = PushSubscribeOptions.builder()
                .stream(STREAM)
                .durable(DURABLE)
                .bind(true)
                .build();
            _testPushDurableSubSync(deliverSubject, nc, js, () -> js.subscribe(null, options2));
            _testPushDurableSubAsync(js, dispatcher, (d, h) -> js.subscribe(null, d, h, false, options2));

            // bind short form
            PushSubscribeOptions options3 = PushSubscribeOptions.bind(STREAM, DURABLE);
            _testPushDurableSubSync(deliverSubject, nc, js, () -> js.subscribe(null, options3));
            _testPushDurableSubAsync(js, dispatcher, (d, h) -> js.subscribe(null, d, h, false, options3));
        });
    }

    private interface SubscriptionSupplier {
        JetStreamSubscription get() throws IOException, JetStreamApiException;
    }

    private interface SubscriptionSupplierAsync {
        JetStreamSubscription get(Dispatcher dispatcher, MessageHandler handler) throws IOException, JetStreamApiException;
    }

    private void _testPushDurableSubSync(String deliverSubject, Connection nc, JetStream js, SubscriptionSupplier supplier) throws InterruptedException, TimeoutException, IOException, JetStreamApiException {
        // publish some messages
        jsPublish(js, SUBJECT, 1, 5);

        JetStreamSubscription sub = supplier.get();
        assertSubscription(sub, STREAM, DURABLE, deliverSubject, false);

        // read what is available
        List<Message> messages = readMessagesAck(sub);
        int total = messages.size();
        validateRedAndTotal(5, messages.size(), 5, total);

        // read again, nothing should be there
        messages = readMessagesAck(sub);
        total += messages.size();
        validateRedAndTotal(0, messages.size(), 5, total);

        sub.unsubscribe();
        nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

        // re-subscribe
        sub = supplier.get();
        nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

        // read again, nothing should be there
        messages = readMessagesAck(sub);
        total += messages.size();
        validateRedAndTotal(0, messages.size(), 5, total);

        sub.unsubscribe();
        nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server
        sleep(250); // make sure the unsub happens
    }

    private void _testPushDurableSubAsync(JetStream js, Dispatcher dispatcher, SubscriptionSupplierAsync supplier) throws IOException, JetStreamApiException, InterruptedException {
        // publish some messages
        jsPublish(js, SUBJECT, 5);

        CountDownLatch msgLatch = new CountDownLatch(5);
        AtomicInteger received = new AtomicInteger();

        MessageHandler handler = (Message msg) -> {
            received.incrementAndGet();
            msg.ack();
            msgLatch.countDown();
        };

        // Subscribe using the handler
        JetStreamSubscription sub = supplier.get(dispatcher, handler);

        // Wait for messages to arrive using the countdown latch.
        awaitAndAssert(msgLatch);

        dispatcher.unsubscribe(sub);
        sleep(250); // make sure the unsub happens

        assertEquals(5, received.get());
    }

    @Test
    public void testCantPullOnPushSub() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            createDefaultTestStream(nc);

            JetStreamSubscription sub = js.subscribe(SUBJECT);
            assertSubscription(sub, STREAM, null, null, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            assertCantPullOnPushSub(sub);
            sub.unsubscribe();

            PushSubscribeOptions pso = PushSubscribeOptions.builder().ordered(true).build();
            sub = js.subscribe(SUBJECT, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            assertCantPullOnPushSub(sub);
        });
    }

    private void assertCantPullOnPushSub(JetStreamSubscription sub) {
        assertThrows(IllegalStateException.class, () -> sub.pull(1));
        assertThrows(IllegalStateException.class, () -> sub.pullNoWait(1));
        assertThrows(IllegalStateException.class, () -> sub.pullExpiresIn(1, Duration.ofSeconds(1)));
        assertThrows(IllegalStateException.class, () -> sub.pullExpiresIn(1, 1000));
        assertThrows(IllegalStateException.class, () -> sub.fetch(1, 1000));
        assertThrows(IllegalStateException.class, () -> sub.fetch(1, Duration.ofSeconds(1)));
        assertThrows(IllegalStateException.class, () -> sub.iterate(1, 1000));
        assertThrows(IllegalStateException.class, () -> sub.iterate(1, Duration.ofSeconds(1)));
    }

    @Test
    public void testHeadersOnly() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();

            // create the stream.
            createDefaultTestStream(nc);

            PushSubscribeOptions pso = ConsumerConfiguration.builder().headersOnly(true).buildPushSubscribeOptions();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            jsPublish(js, SUBJECT, 5);

            List<Message> messages = readMessagesAck(sub, Duration.ZERO, 5);
            assertEquals(5, messages.size());
            assertEquals(0, messages.get(0).getData().length);
            assertNotNull(messages.get(0).getHeaders());
            assertEquals("6", messages.get(0).getHeaders().getFirst(NatsJetStreamConstants.MSG_SIZE_HDR));
        });
    }

    @Test
    public void testAcks() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            createDefaultTestStream(nc);

            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackWait(Duration.ofMillis(1500)).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // TERM
            jsPublish(js, SUBJECT, "TERM", 1);

            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("TERM1", data);
            message.term();

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            // Ack Wait timeout
            jsPublish(js, SUBJECT, "WAIT", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("WAIT1", data);
            sleep(2000);
            message.ack(); // this ack came too late so will be ignored

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("WAIT1", data);

            // In Progress
            jsPublish(js, SUBJECT, "PRO", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("PRO1", data);
            message.inProgress();
            sleep(750);
            message.inProgress();
            sleep(750);
            message.inProgress();
            sleep(750);
            message.inProgress();
            sleep(750);
            message.ack();

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            // ACK Sync
            jsPublish(js, SUBJECT, "ACKSYNC", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("ACKSYNC1", data);
            message.ackSync(Duration.ofSeconds(1));

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            // NAK
            jsPublish(js, SUBJECT, "NAK", 1, 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK1", data);
            message.nak();

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK1", data);
            message.ack();

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            jsPublish(js, SUBJECT, "NAK", 2, 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK2", data);
            message.nakWithDelay(3000);

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            message = sub.nextMessage(Duration.ofSeconds(3000));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK2", data);
            message.ack();

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            jsPublish(js, SUBJECT, "NAK", 3, 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK3", data);
            message.nakWithDelay(Duration.ofSeconds(3)); // coverage to use both nakWithDelay

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            message = sub.nextMessage(Duration.ofSeconds(3000));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK3", data);
            message.ack();

            assertNull(sub.nextMessage(Duration.ofMillis(500)));
        });
    }

    @Test
    public void testDeliveryPolicy() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(jsm, STREAM, SUBJECT_STAR);

            String subjectA = subjectDot("A");
            String subjectB = subjectDot("B");

            js.publish(subjectA, dataBytes(1));
            js.publish(subjectA, dataBytes(2));
            sleep(1500);
            js.publish(subjectA, dataBytes(3));
            js.publish(subjectB, dataBytes(91));
            js.publish(subjectB, dataBytes(92));

            jsm.deleteMessage(STREAM, 4);

            // DeliverPolicy.All
            PushSubscribeOptions pso = PushSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.All).build())
                    .build();
            JetStreamSubscription sub = js.subscribe(subjectA, pso);
            Message m1 = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m1, 1);
            Message m2 = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m2, 2);
            Message m3 = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m3, 3);

            // DeliverPolicy.Last
            pso = PushSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.Last).build())
                    .build();
            sub = js.subscribe(subjectA, pso);
            Message m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 3);
            assertNull(sub.nextMessage(Duration.ofMillis(200)));

            // DeliverPolicy.New - No new messages between subscribe and next message
            pso = PushSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.New).build())
                    .build();
            sub = js.subscribe(subjectA, pso);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

            // DeliverPolicy.New - New message between subscribe and next message
            sub = js.subscribe(subjectA, pso);
            js.publish(subjectA, dataBytes(4));
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 4);

            // DeliverPolicy.ByStartSequence
            pso = PushSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder()
                            .deliverPolicy(DeliverPolicy.ByStartSequence)
                            .startSequence(3)
                            .build())
                    .build();
            sub = js.subscribe(subjectA, pso);
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 3);
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 4);

            // DeliverPolicy.ByStartTime
            pso = PushSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder()
                            .deliverPolicy(DeliverPolicy.ByStartTime)
                            .startTime(m3.metaData().timestamp().minusSeconds(1))
                            .build())
                    .build();
            sub = js.subscribe(subjectA, pso);
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 3);
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 4);

            // DeliverPolicy.LastPerSubject
            pso = PushSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder()
                            .deliverPolicy(DeliverPolicy.LastPerSubject)
                            .filterSubject(subjectA)
                            .build())
                    .build();
            sub = js.subscribe(subjectA, pso);
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 4);

            // DeliverPolicy.ByStartSequence with a deleted record
            PublishAck pa4 = js.publish(subjectA, dataBytes(4));
            PublishAck pa5 = js.publish(subjectA, dataBytes(5));
            js.publish(subjectA, dataBytes(6));
            jsm.deleteMessage(STREAM, pa4.getSeqno());
            jsm.deleteMessage(STREAM, pa5.getSeqno());

            pso = PushSubscribeOptions.builder()
                .configuration(ConsumerConfiguration.builder()
                    .deliverPolicy(DeliverPolicy.ByStartSequence)
                    .startSequence(pa4.getSeqno())
                    .build())
                .build();
            sub = js.subscribe(subjectA, pso);
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 6);
        });
    }

    private void assertMessage(Message m, int i) {
        assertNotNull(m);
        assertEquals(data(i), new String(m.getData()));
    }

    @Test
    public void testPushSyncFlowControl() throws Exception {
        AtomicInteger fcps = new AtomicInteger();

        ErrorListener el = new ErrorListener() {
            @Override
            public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
                fcps.incrementAndGet();
            }
        };

        Options.Builder ob = new Options.Builder().errorListener(el);

        runInJsServer(ob, nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            createDefaultTestStream(nc);

            byte[] data = new byte[8192];

            int MSG_COUNT = 1000;

            // publish some messages
            for (int x = 100_000; x < MSG_COUNT + 100_000; x++) {
                byte[] fill = (""+ x).getBytes();
                System.arraycopy(fill, 0, data, 0, 6);
                js.publish(NatsMessage.builder().subject(SUBJECT).data(data).build());
            }

            // reset the counters
            int count = 0;
            Set<String> set = new HashSet<>();

            ConsumerConfiguration cc = ConsumerConfiguration.builder().flowControl(1000).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            for (int x = 0; x < MSG_COUNT; x++) {
                Message msg = sub.nextMessage(1000);
                String id = new String(Arrays.copyOf(msg.getData(), 6));
                if (set.add(id)) {
                    count++;
                }
                msg.ack();
            }

            assertEquals(MSG_COUNT, count);
            assertTrue(fcps.get() > 0);

            // coverage for subscribe options heartbeat directly
            cc = ConsumerConfiguration.builder().idleHeartbeat(100).build();
            pso = PushSubscribeOptions.builder().configuration(cc).build();
            js.subscribe(SUBJECT, pso);
        });
    }

    // ------------------------------------------------------------------------------------------
    // this allows me to intercept messages before it gets to the connection queue
    // which is before the messages is available for nextMessage or before
    // it gets dispatched to a handler.
    static class OrderedTestPushStatusMessageManager extends PushStatusMessageManager {

        public OrderedTestPushStatusMessageManager(NatsConnection conn, SubscribeOptions so, ConsumerConfiguration cc, boolean queueMode, boolean syncMode) {
            super(conn, so, cc, queueMode, syncMode);
        }

        @Override
        NatsMessage beforeQueueProcessor(NatsMessage msg) {
            msg = super.beforeQueueProcessor(msg);
            if (msg != null && msg.isJetStream()) {
                long ss = msg.metaData().streamSequence();
                long cs = msg.metaData().consumerSequence();
                if ((ss == 2 && cs == 2) || (ss == 5 && cs == 4) || (ss == 8 && cs == 2) || (ss == 11 && cs == 4)) {
                    return null;
                }
            }
            return msg;
        }
    }

    @Test
    @Timeout(value = 60)
    public void testOrdered() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, subject(1), subject(2));

            NatsJetStream.PUSH_STATUS_MANAGER_FACTORY = OrderedTestPushStatusMessageManager::new;

            PushSubscribeOptions pso = PushSubscribeOptions.builder().ordered(true).build();
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, QUEUE, pso));
            assertTrue(iae.getMessage().contains(JsSubOrderedNotAllowOnQueues.id()));

            JetStreamSubscription sub = js.subscribe(subject(1), pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server
            sleep(1000);

            // publish after interceptor is set before messages come in
            jsPublish(js, subject(1), 101, 6);

            int streamSeq = 1;
            int[] expectedCnsmSeqs = new int[] {1, 1, 2, 3, 1, 2};

            while (streamSeq < 7) {
                Message m = sub.nextMessage(Duration.ofSeconds(1)); // use duration version here for coverage
                if (m != null) {
                    assertEquals(streamSeq, m.metaData().streamSequence());
                    assertEquals(expectedCnsmSeqs[streamSeq-1], m.metaData().consumerSequence());
                    ++streamSeq;
                }
            }

            sub.unsubscribe(1);

            // ----------------------------------------------------------------------------------------------------
            // THIS IS ACTUALLY TESTING ASYNC SO I DON'T HAVE TO SETUP THE INTERCEPTOR IN OTHER CODE
            // ----------------------------------------------------------------------------------------------------
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

            Dispatcher d = nc.createDispatcher();
            sub = js.subscribe(subject(2), d, handler, false, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server
            sleep(1000);

            // publish after sub to make sure interceptor is set before messages come in
            jsPublish(js, subject(2), 201, 6);

            awaitAndAssert(msgLatch);

            while (streamSeq < 13) {
                int flagIdx = streamSeq - 7;
                assertEquals(streamSeq, ssFlags[flagIdx].get());
                assertEquals(expectedCnsmSeqs[flagIdx], csFlags[flagIdx].get());
                ++streamSeq;
            }

            d.unsubscribe(sub);
        });
    }
}
