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
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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

            // no subject so stream and durable are required
            PushSubscribeOptions options2 = PushSubscribeOptions.builder()
                .stream(STREAM)
                .durable(DURABLE)
                .deliverSubject(deliverSubject)
                .build();
            _testPushDurableSubSync(deliverSubject, nc, js, () -> js.subscribe(null, options2));
            _testPushDurableSubAsync(js, dispatcher, (d, h) -> js.subscribe(null, d, h, false, options2));

            // bind long form
            PushSubscribeOptions options3 = PushSubscribeOptions.builder()
                .stream(STREAM)
                .durable(DURABLE)
                .bind(true)
                .deliverSubject(deliverSubject)
                .build();
            _testPushDurableSubSync(deliverSubject, nc, js, () -> js.subscribe(null, options3));
            _testPushDurableSubAsync(js, dispatcher, (d, h) -> js.subscribe(null, d, h, false, options3));

            // bind short form
            PushSubscribeOptions options4 = PushSubscribeOptions.bind(STREAM, DURABLE);
            _testPushDurableSubSync(deliverSubject, nc, js, () -> js.subscribe(null, options4));
            _testPushDurableSubAsync(js, dispatcher, (d, h) -> js.subscribe(null, d, h, false, options4));

            // bind shortest form
            _testPushDurableSubSync(deliverSubject, nc, js, () -> js.bindSubscribePushSync(STREAM, DURABLE));
            _testPushDurableSubAsync(js, dispatcher, (d, h) -> js.bindSubscribePushAsync(STREAM, DURABLE, d, h, false));
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
        msgLatch.await(10, TimeUnit.SECONDS);

        dispatcher.unsubscribe(sub);

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

            // this should exception, can't pull on a push sub
            assertThrows(IllegalStateException.class, () -> sub.pull(1));
            assertThrows(IllegalStateException.class, () -> sub.pullNoWait(1));
            assertThrows(IllegalStateException.class, () -> sub.pullExpiresIn(1, Duration.ofSeconds(1)));
            assertThrows(IllegalStateException.class, () -> sub.pullExpiresIn(1, 1000));
            assertThrows(IllegalStateException.class, () -> sub.fetch(1, 1000));
            assertThrows(IllegalStateException.class, () -> sub.fetch(1, Duration.ofSeconds(1)));
            assertThrows(IllegalStateException.class, () -> sub.iterate(1, 1000));
            assertThrows(IllegalStateException.class, () -> sub.iterate(1, Duration.ofSeconds(1)));
        });
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

            // NAK
            jsPublish(js, SUBJECT, "NAK", 1);

            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("NAK1", data);
            message.nak();

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK1", data);
            message.ack();

            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

            // TERM
            jsPublish(js, SUBJECT, "TERM", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("TERM1", data);
            message.term();

            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

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

            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

            // ACK Sync
            jsPublish(js, SUBJECT, "ACKSYNC", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("ACKSYNC1", data);
            message.ackSync(Duration.ofSeconds(1));

            assertNull(sub.nextMessage(Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testDeliveryPolicy() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT_STAR);

            String subjectA = subjectDot("A");
            String subjectB = subjectDot("B");

            js.publish(subjectA, dataBytes(1));
            js.publish(subjectA, dataBytes(2));
            sleep(1500);
            js.publish(subjectA, dataBytes(3));
            js.publish(subjectB, dataBytes(91));
            js.publish(subjectB, dataBytes(92));

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
        });
    }
}
