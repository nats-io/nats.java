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
import io.nats.client.support.Listener;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.client.support.NatsJetStreamClientError.JsSubPushAsyncCantSetPending;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPushTests extends JetStreamTestBase {

    @Test
    public void testPushEphemeralNullDeliver() throws Exception {
        _testPushEphemeral(null);
    }

    @Test
    public void testPushEphemeralWithDeliver() throws Exception {
        _testPushEphemeral(random());
    }

    private void _testPushEphemeral(String deliverSubject) throws Exception {
        runInShared((nc, ctx) -> {
            // publish some messages
            jsPublish(ctx.js, ctx.subject(), 1, 5);

            // Build our subscription options.
            PushSubscribeOptions options = PushSubscribeOptions.builder().deliverSubject(deliverSubject).build();

            // Subscription 1
            JetStreamSubscription sub1 = ctx.js.subscribe(ctx.subject(), options);
            assertSubscription(sub1, ctx.stream, null, deliverSubject, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available
            List<Message> messages1 = readMessagesAck(sub1);
            int total = messages1.size();
            validateRedAndTotal(5, messages1.size(), 5, total);

            // read again, nothing should be there
            List<Message> messages0 = readMessagesAck(sub1);
            total += messages0.size();
            validateRedAndTotal(0, messages0.size(), 5, total);

            // needed for deliver subject version b/c the sub
            // would be identical. without ds, the ds is generated each
            // time so is unique
            unsubscribeEnsureNotBound(sub1);

            // Subscription 2
            JetStreamSubscription sub2 = ctx.js.subscribe(ctx.subject(), options);
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

            unsubscribeEnsureNotBound(sub2);

            // Subscription 3 testing null timeout
            JetStreamSubscription sub3 = ctx.js.subscribe(ctx.subject(), options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server
            sleep(1000); // give time to make sure the messages get to the client

            messages0 = readMessagesAck(sub3, null);
            validateRedAndTotal(5, messages0.size(), 5, 5);

            // Subscription 4 testing timeout <= 0 duration / millis
            JetStreamSubscription sub4 = ctx.js.subscribe(ctx.subject(), options);
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
        _testPushDurable(false);
    }

    @Test
    public void testPushDurableWithDeliver() throws Exception {
        _testPushDurable(true);
    }

    private void _testPushDurable(boolean useDeliverSubject) throws Exception {
        runInSharedCustom((nc, ctx) -> {
            String subjectDotGt = random() + ".>";
            ctx.createOrReplaceStream(subjectDotGt);

            String stream = ctx.stream;

            // For async, create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            // normal, no bind
            _testPushDurableSubSync(ctx, stream, subjectDotGt, useDeliverSubject, false, (s, cc) -> {
                PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .durable(cc.getDurable())
                    .deliverSubject(cc.getDeliverSubject())
                    .build();
                return ctx.js.subscribe(s, options);
            });

            _testPushDurableSubAsync(ctx, dispatcher, stream, subjectDotGt, useDeliverSubject, false, (s, d, h, cc) -> {
                PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .durable(cc.getDurable())
                    .deliverSubject(cc.getDeliverSubject())
                    .build();
                return ctx.js.subscribe(s, d, h, false, options);
            });

            // use configuration, no bind
            _testPushDurableSubSync(ctx, stream, subjectDotGt, useDeliverSubject, false, (s, cc) -> {
                PushSubscribeOptions options = PushSubscribeOptions.builder().configuration(cc).build();
                return ctx.js.subscribe(s, options);
            });

            _testPushDurableSubAsync(ctx, dispatcher, stream, subjectDotGt, useDeliverSubject, false, (s, d, h, cc) -> {
                PushSubscribeOptions options = PushSubscribeOptions.builder().configuration(cc).build();
                return ctx.js.subscribe(s, d, h, false, options);
            });

            if (useDeliverSubject) {
                // bind long form
                _testPushDurableSubSync(ctx, stream, subjectDotGt, true, true, (s, cc) -> {
                    PushSubscribeOptions options = PushSubscribeOptions.builder().stream(stream).durable(cc.getDurable()).bind(true).build();
                    return ctx.js.subscribe(s, options);
                });

                _testPushDurableSubAsync(ctx, dispatcher, stream, subjectDotGt, true, true, (s, d, h, cc) -> {
                    PushSubscribeOptions options = PushSubscribeOptions.builder().stream(stream).durable(cc.getDurable()).bind(true).build();
                    return ctx.js.subscribe(s, d, h, false, options);
                });

                // bind short form
                _testPushDurableSubSync(ctx, stream, subjectDotGt, true, true, (s, cc) -> {
                    PushSubscribeOptions options = PushSubscribeOptions.bind(stream, cc.getDurable());
                    return ctx.js.subscribe(s, options);
                });

                _testPushDurableSubAsync(ctx, dispatcher, stream, subjectDotGt, true, true, (s, d, h, cc) -> {
                    PushSubscribeOptions options = PushSubscribeOptions.bind(stream, cc.getDurable());
                    return ctx.js.subscribe(s, d, h, false, options);
                });
            }
        });
    }

    private interface SubscriptionSupplier {
        JetStreamSubscription get(String subject, ConsumerConfiguration cc) throws IOException, JetStreamApiException;
    }

    private interface SubscriptionSupplierAsync {
        JetStreamSubscription get(String subject, Dispatcher dispatcher, MessageHandler handler, ConsumerConfiguration cc) throws IOException, JetStreamApiException;
    }

    private void _testPushDurableSubSync(JetStreamTestingContext ctx, String stream, String subjectDotGt, boolean useDeliverSubject, boolean bind, SubscriptionSupplier supplier) throws Exception {
        String subject = subjectDotGt.replace(">", random());
        String durable = random();
        String deliverSubject = useDeliverSubject ? random() : null;
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
            .durable(durable)
            .deliverSubject(deliverSubject)
            .filterSubject(subject)
            .build();

        if (bind) {
            ctx.jsm.addOrUpdateConsumer(stream, cc);
        }

        // publish some messages
        jsPublish(ctx.js, subject, 1, 5);

        JetStreamSubscription sub = supplier.get(subject, cc);
        assertSubscription(sub, stream, durable, deliverSubject, false);

        // read what is available
        List<Message> messages = readMessagesAck(sub);
        int total = messages.size();
        validateRedAndTotal(5, messages.size(), 5, total);

        // read again, nothing should be there
        messages = readMessagesAck(sub);
        total += messages.size();
        validateRedAndTotal(0, messages.size(), 5, total);

        unsubscribeEnsureNotBound(sub);

        // re-subscribe
        sub = supplier.get(subject, cc);

        // read again, nothing should be there
        messages = readMessagesAck(sub);
        total += messages.size();
        validateRedAndTotal(0, messages.size(), 5, total);

        unsubscribeEnsureNotBound(sub);
    }

    private void _testPushDurableSubAsync(JetStreamTestingContext ctx, Dispatcher dispatcher, String stream, String subjectDotGt, boolean useDeliverSubject, boolean bind, SubscriptionSupplierAsync supplier) throws IOException, JetStreamApiException, InterruptedException {
        String subject = subjectDotGt.replace(">", random());
        String deliverSubject = useDeliverSubject ? random() : null;
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
            .durable(random())
            .deliverSubject(deliverSubject)
            .filterSubject(subject)
            .build();
        if (bind) {
            ctx.jsm.addOrUpdateConsumer(stream, cc);
        }

        // publish some messages
        jsPublish(ctx.js, subject, 5);

        CountDownLatch msgLatch = new CountDownLatch(5);
        AtomicInteger received = new AtomicInteger();

        MessageHandler handler = (Message msg) -> {
            received.incrementAndGet();
            msg.ack();
            msgLatch.countDown();
        };

        // Subscribe using the handler
        JetStreamSubscription sub = supplier.get(subject, dispatcher, handler, cc);

        // Wait for messages to arrive using the countdown latch.
        awaitAndAssert(msgLatch);

        unsubscribeEnsureNotBound(dispatcher, sub);

        assertEquals(5, received.get());
    }

    @Test
    public void testCantPullOnPushSub() throws Exception {
        runInShared((nc, ctx) -> {
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject());
            assertSubscription(sub, ctx.stream, null, null, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            assertCantPullOnPushSub(sub);
            unsubscribeEnsureNotBound(sub);

            PushSubscribeOptions pso = PushSubscribeOptions.builder().ordered(true).build();
            sub = ctx.js.subscribe(ctx.subject(), pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            assertCantPullOnPushSub(sub);
        });
    }

    private void assertCantPullOnPushSub(JetStreamSubscription sub) {
        assertThrows(IllegalStateException.class, () -> sub.pull(1));
        assertThrows(IllegalStateException.class, () -> sub.pull(PullRequestOptions.builder(1).build()));
        assertThrows(IllegalStateException.class, () -> sub.pullNoWait(1));
        assertThrows(IllegalStateException.class, () -> sub.pullNoWait(1, Duration.ofSeconds(1)));
        assertThrows(IllegalStateException.class, () -> sub.pullNoWait(1, 1000));
        assertThrows(IllegalStateException.class, () -> sub.pullExpiresIn(1, Duration.ofSeconds(1)));
        assertThrows(IllegalStateException.class, () -> sub.pullExpiresIn(1, 1000));
        assertThrows(IllegalStateException.class, () -> sub.fetch(1, 1000));
        assertThrows(IllegalStateException.class, () -> sub.fetch(1, Duration.ofSeconds(1)));
        assertThrows(IllegalStateException.class, () -> sub.iterate(1, 1000));
        assertThrows(IllegalStateException.class, () -> sub.iterate(1, Duration.ofSeconds(1)));
        assertThrows(IllegalStateException.class, () -> sub.reader(1, 2));
    }

    @Test
    public void testHeadersOnly() throws Exception {
        runInShared((nc, ctx) -> {
            PushSubscribeOptions pso = ConsumerConfiguration.builder().headersOnly(true).buildPushSubscribeOptions();
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            jsPublish(ctx.js, ctx.subject(), 5);

            List<Message> messages = readMessagesAck(sub, Duration.ZERO, 5);
            assertEquals(5, messages.size());
            assertEquals(0, messages.get(0).getData().length);
            assertNotNull(messages.get(0).getHeaders());
            assertEquals("6", messages.get(0).getHeaders().getFirst(NatsJetStreamConstants.MSG_SIZE_HDR));
        });
    }

    @Test
    public void testAcks() throws Exception {
        runInShared((nc, ctx) -> {
            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackWait(Duration.ofMillis(1500)).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // TERM
            jsPublish(ctx.js, ctx.subject(), "TERM", 1);

            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("TERM1", data);
            message.term();
            assertEquals(AckType.AckTerm, message.lastAck());

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            // Ack Wait timeout
            jsPublish(ctx.js, ctx.subject(), "WAIT", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("WAIT1", data);
            sleep(2000);
            message.ack(); // this ack came too late so will be ignored
            assertEquals(AckType.AckAck, message.lastAck());

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("WAIT1", data);

            // In Progress
            jsPublish(ctx.js, ctx.subject(), "PRO", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("PRO1", data);
            message.inProgress();
            assertEquals(AckType.AckProgress, message.lastAck());
            sleep(750);
            message.inProgress();
            assertEquals(AckType.AckProgress, message.lastAck());
            sleep(750);
            message.inProgress();
            assertEquals(AckType.AckProgress, message.lastAck());
            sleep(750);
            message.inProgress();
            assertEquals(AckType.AckProgress, message.lastAck());
            sleep(750);
            message.ack();
            assertEquals(AckType.AckAck, message.lastAck());

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            // ACK Sync
            jsPublish(ctx.js, ctx.subject(), "ACKSYNC", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("ACKSYNC1", data);
            message.ackSync(Duration.ofSeconds(1));
            assertEquals(AckType.AckAck, message.lastAck());

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            // NAK
            jsPublish(ctx.js, ctx.subject(), "NAK", 1, 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK1", data);
            message.nak();
            assertEquals(AckType.AckNak, message.lastAck());

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK1", data);
            message.ack();
            assertEquals(AckType.AckAck, message.lastAck());

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            jsPublish(ctx.js, ctx.subject(), "NAK", 2, 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK2", data);
            message.nakWithDelay(3000);
            assertEquals(AckType.AckNak, message.lastAck());

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            message = sub.nextMessage(Duration.ofSeconds(3000));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK2", data);
            message.ack();
            assertEquals(AckType.AckAck, message.lastAck());

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            jsPublish(ctx.js, ctx.subject(), "NAK", 3, 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK3", data);
            message.nakWithDelay(Duration.ofSeconds(3)); // coverage to use both nakWithDelay
            assertEquals(AckType.AckNak, message.lastAck());

            assertNull(sub.nextMessage(Duration.ofMillis(500)));

            message = sub.nextMessage(Duration.ofSeconds(3000));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK3", data);
            message.ack();
            assertEquals(AckType.AckAck, message.lastAck());

            assertNull(sub.nextMessage(Duration.ofMillis(500)));
        });
    }

    @Test
    public void testDeliveryPolicy() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            String subject = ctx.subject();
            String subjectStar = subjectStar(subject);
            ctx.createOrReplaceStream(subjectStar);

            String subjectA = subjectDot(subject, "A");
            String subjectB = subjectDot(subject, "B");

            ctx.js.publish(subjectA, dataBytes(1));
            ctx.js.publish(subjectA, dataBytes(2));
            sleep(1500);
            ctx.js.publish(subjectA, dataBytes(3));
            ctx.js.publish(subjectB, dataBytes(91));
            ctx.js.publish(subjectB, dataBytes(92));

            ctx.jsm.deleteMessage(ctx.stream, 4);

            // DeliverPolicy.All
            PushSubscribeOptions pso = PushSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.All).build())
                    .build();
            JetStreamSubscription sub = ctx.js.subscribe(subjectA, pso);
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
            sub = ctx.js.subscribe(subjectA, pso);
            Message m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 3);
            assertNull(sub.nextMessage(Duration.ofMillis(200)));

            // DeliverPolicy.New - No new messages between subscribe and next message
            pso = PushSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.New).build())
                    .build();
            sub = ctx.js.subscribe(subjectA, pso);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

            // DeliverPolicy.New - New message between subscribe and next message
            sub = ctx.js.subscribe(subjectA, pso);
            ctx.js.publish(subjectA, dataBytes(4));
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 4);

            // DeliverPolicy.ByStartSequence
            pso = PushSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder()
                            .deliverPolicy(DeliverPolicy.ByStartSequence)
                            .startSequence(3)
                            .build())
                    .build();
            sub = ctx.js.subscribe(subjectA, pso);
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
            sub = ctx.js.subscribe(subjectA, pso);
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
            sub = ctx.js.subscribe(subjectA, pso);
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertMessage(m, 4);

            // DeliverPolicy.ByStartSequence with a deleted record
            PublishAck pa4 = ctx.js.publish(subjectA, dataBytes(4));
            PublishAck pa5 = ctx.js.publish(subjectA, dataBytes(5));
            ctx.js.publish(subjectA, dataBytes(6));
            ctx.jsm.deleteMessage(ctx.stream, pa4.getSeqno());
            ctx.jsm.deleteMessage(ctx.stream, pa5.getSeqno());

            pso = PushSubscribeOptions.builder()
                .configuration(ConsumerConfiguration.builder()
                    .deliverPolicy(DeliverPolicy.ByStartSequence)
                    .startSequence(pa4.getSeqno())
                    .build())
                .build();
            sub = ctx.js.subscribe(subjectA, pso);
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
        Listener listener = new Listener();
        runInSharedOwnNc(listener, (nc, ctx) -> {
            byte[] data = new byte[1024*10];
            int MSG_COUNT = 1000;

            // publish some messages
            for (int x = 100_000; x < MSG_COUNT + 100_000; x++) {
                byte[] fill = ("" + x).getBytes();
                System.arraycopy(fill, 0, data, 0, 6);
                ctx.js.publish(NatsMessage.builder().subject(ctx.subject()).data(data).build());
            }

            // reset the counters
            Set<String> set = new HashSet<>();

            ConsumerConfiguration cc = ConsumerConfiguration.builder().flowControl(1000).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), pso);
            for (int x = 0; x < MSG_COUNT; x++) {
                Message msg = sub.nextMessage(1000);
                set.add(new String(Arrays.copyOf(msg.getData(), 6)));
                msg.ack();
                sleep(5); // slow it down, easier to get flow control
            }

            assertEquals(MSG_COUNT, set.size());
            assertTrue(listener.getFlowControlCount() > 0);

            // coverage for subscribe options heartbeat directly
            cc = ConsumerConfiguration.builder().idleHeartbeat(100).build();
            pso = PushSubscribeOptions.builder().configuration(cc).build();
            ctx.js.subscribe(ctx.subject(), pso);
        });
    }

    @Test
    public void testPendingLimits() throws Exception {
        runInShared((nc, ctx) -> {
            int customMessageLimit = 1000;
            int customByteLimit = 1024 * 1024;

            PushSubscribeOptions psoDefaultSync = PushSubscribeOptions.builder()
                .build();

            PushSubscribeOptions psoCustomSync = PushSubscribeOptions.builder()
                .pendingMessageLimit(customMessageLimit)
                .pendingByteLimit(customByteLimit)
                .build();

            PushSubscribeOptions psoCustomSyncUnlimited0 = PushSubscribeOptions.builder()
                .pendingMessageLimit(0)
                .pendingByteLimit(0)
                .build();

            PushSubscribeOptions psoCustomSyncUnlimitedUnlimitedNegative = PushSubscribeOptions.builder()
                .pendingMessageLimit(-1)
                .pendingByteLimit(-1)
                .build();

            JetStreamSubscription syncSub = ctx.js.subscribe(ctx.subject(), psoDefaultSync);
            assertEquals(Consumer.DEFAULT_MAX_MESSAGES, syncSub.getPendingMessageLimit());
            assertEquals(Consumer.DEFAULT_MAX_BYTES, syncSub.getPendingByteLimit());

            syncSub = ctx.js.subscribe(ctx.subject(), psoCustomSync);
            assertEquals(customMessageLimit, syncSub.getPendingMessageLimit());
            assertEquals(customByteLimit, syncSub.getPendingByteLimit());

            syncSub = ctx.js.subscribe(ctx.subject(), psoCustomSyncUnlimited0);
            assertEquals(0, syncSub.getPendingMessageLimit());
            assertEquals(0, syncSub.getPendingByteLimit());

            syncSub = ctx.js.subscribe(ctx.subject(), psoCustomSyncUnlimitedUnlimitedNegative);
            assertEquals(0, syncSub.getPendingMessageLimit());
            assertEquals(0, syncSub.getPendingByteLimit());

            Dispatcher d = nc.createDispatcher();
            d.setPendingLimits(customMessageLimit, customByteLimit);
            assertEquals(customMessageLimit, d.getPendingMessageLimit());
            assertEquals(customByteLimit, d.getPendingByteLimit());

            PushSubscribeOptions psoAsyncDefault = PushSubscribeOptions.builder().build();
            PushSubscribeOptions psoAsyncNonDefaultValid = PushSubscribeOptions.builder()
                .pendingMessageLimit(Consumer.DEFAULT_MAX_MESSAGES)
                .pendingByteLimit(Consumer.DEFAULT_MAX_BYTES)
                .build();

            JetStreamSubscription subAsync = ctx.js.subscribe(ctx.subject(), d, m -> {}, false, psoAsyncDefault);
            assertEquals(Consumer.DEFAULT_MAX_MESSAGES, subAsync.getPendingMessageLimit());
            assertEquals(Consumer.DEFAULT_MAX_BYTES, subAsync.getPendingByteLimit());

            subAsync = ctx.js.subscribe(ctx.subject(), d, m -> {}, false, psoAsyncNonDefaultValid);
            assertEquals(Consumer.DEFAULT_MAX_MESSAGES, subAsync.getPendingMessageLimit());
            assertEquals(Consumer.DEFAULT_MAX_BYTES, subAsync.getPendingByteLimit());

            PushSubscribeOptions psoAsyncNopeMessages = PushSubscribeOptions.builder()
                .pendingMessageLimit(customMessageLimit)
                .build();

            PushSubscribeOptions psoAsyncNopeBytes = PushSubscribeOptions.builder()
                .pendingByteLimit(customByteLimit)
                .build();

            PushSubscribeOptions psoAsyncNope2Messages = PushSubscribeOptions.builder()
                .pendingMessageLimit(0)
                .build();

            PushSubscribeOptions psoAsyncNope2Bytes = PushSubscribeOptions.builder()
                .pendingByteLimit(0)
                .build();

            assertClientError(JsSubPushAsyncCantSetPending, () -> ctx.js.subscribe(random(), d, m ->{}, false, psoAsyncNopeMessages));
            assertClientError(JsSubPushAsyncCantSetPending, () -> ctx.js.subscribe(random(), d, m ->{}, false, psoAsyncNopeBytes));
            assertClientError(JsSubPushAsyncCantSetPending, () -> ctx.js.subscribe(random(), d, m ->{}, false, psoAsyncNope2Messages));
            assertClientError(JsSubPushAsyncCantSetPending, () -> ctx.js.subscribe(random(), d, m ->{}, false, psoAsyncNope2Bytes));
        });
    }
}
