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

import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPushTests extends JetStreamTestBase {

    @ParameterizedTest
    @NullSource // tests null or no deliver subject
    @ValueSource(strings = {DELIVER}) // tests actual deliver subject
    public void testPushEphemeral(String deliverSubject) throws Exception {
        runInJsServer(nc -> {
            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // publish some messages
            jsPublish(js, SUBJECT, 1, 5);

            // Build our subscription options.
            PushSubscribeOptions options = PushSubscribeOptions.builder().deliverSubject(deliverSubject).build();

            // Subscription 1
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            assertSubscription(sub, STREAM, null, deliverSubject, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available
            List<Message> messages1 = readMessagesAck(sub);
            int total = messages1.size();
            validateRedAndTotal(5, messages1.size(), 5, total);

            // read again, nothing should be there
            List<Message> messages0 = readMessagesAck(sub);
            total += messages0.size();
            validateRedAndTotal(0, messages0.size(), 5, total);

            // Subscription 2
            sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available, same messages
            List<Message> messages2 = readMessagesAck(sub);
            total = messages2.size();
            validateRedAndTotal(5, messages2.size(), 5, total);

            // read again, nothing should be there
            messages0 = readMessagesAck(sub);
            total += messages0.size();
            validateRedAndTotal(0, messages0.size(), 5, total);

            assertSameMessages(messages1, messages2);
        });
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {DELIVER})
    public void testPushDurable(String deliverSubject) throws Exception {
        runInJsServer(nc -> {
            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // publish some messages
            jsPublish(js, SUBJECT, 1, 5);

            // use ackWait so I don't have to wait forever before re-subscribing
            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackWait(Duration.ofSeconds(3)).build();

            // Build our subscription options.
            PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder()
                    .durable(DURABLE).configuration(cc);
            if (deliverSubject != null) {
                builder.deliverSubject(deliverSubject);
            }
            PushSubscribeOptions options = builder.build();

            // Subscribe.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            assertSubscription(sub, STREAM, DURABLE, deliverSubject, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

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
            sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read again, nothing should be there
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 5, total);
        });
    }

    @Test
    public void testCantPullOnPushSub() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            JetStreamSubscription sub = js.subscribe(SUBJECT);
            assertSubscription(sub, STREAM, null, null, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // this should exception, can't pull on a push sub
            assertThrows(IllegalStateException.class, () -> sub.pull(1));
            assertThrows(IllegalStateException.class, () -> sub.pullNoWait(1));
            // TODO pullExpiresIn
            assertThrows(IllegalStateException.class, () -> sub.pullExpiresIn(1, Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testAcks() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

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
            // Create our JetStream context to receive JetStream messages.
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
}
