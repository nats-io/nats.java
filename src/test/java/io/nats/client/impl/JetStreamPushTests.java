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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPushTests extends JetStreamTestBase {

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {DELIVER})
    public void testPushEphemeral(String deliverSubject) throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // publish some messages
            publish(js, SUBJECT, 1, 5);

            // Build our subscription options.
            PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder();
            if (deliverSubject != null) {
                builder.deliverSubject(deliverSubject);
            }
            PushSubscribeOptions options = builder.build();

            // Subscription 1
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
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
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // publish some messages
            publish(js, SUBJECT, 1, 5);

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
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // this should exception, can't pull on a push sub
            assertThrows(IllegalStateException.class, () -> sub.pull(1));
        });
    }

    @Test
    public void testGetConsumerInfo() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            JetStreamSubscription sub = js.subscribe(SUBJECT);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            ConsumerInfo ci = sub.getConsumerInfo();
            assertEquals(STREAM, ci.getStreamName());
        });
    }

    @Test
    public void testQueueSub() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Setup the subscribers
            // - the PushSubscribeOptions can be re-used since all the subscribers are the same
            // - use a concurrent integer to track all the messages received
            // - have a list of subscribers and threads so I can track them
            PushSubscribeOptions pso = PushSubscribeOptions.builder().durable(DURABLE).build();
            AtomicInteger allReceived = new AtomicInteger();
            List<JsQueueSubscriber> subscribers = new ArrayList<>();
            List<Thread> subThreads = new ArrayList<>();
            for (int id = 1; id <= 3; id++) {
                // setup the subscription
                JetStreamSubscription sub = js.subscribe(SUBJECT, QUEUE, pso);
                // create and track the runnable
                JsQueueSubscriber qs = new JsQueueSubscriber(100, js, sub, allReceived);
                subscribers.add(qs);
                // create, track and start the thread
                Thread t = new Thread(qs);
                subThreads.add(t);
                t.start();
            }
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // create and start the publishing
            Thread pubThread = new Thread(new JsPublisher(js, 100));
            pubThread.start();

            // wait for all threads to finish
            pubThread.join();
            for (Thread t : subThreads) {
                t.join();
            }

            // count
            int count = 0;
            for (JsQueueSubscriber qs : subscribers) {
                int c = qs.thisReceived.get();
                assertTrue(c > 0);
                count += c;
            }

            assertEquals(100, count);
        });
    }

    static class JsPublisher implements Runnable {
        JetStream js;
        int msgCount;

        public JsPublisher(JetStream js, int msgCount) {
            this.js = js;
            this.msgCount = msgCount;
        }

        @Override
        public void run() {
            for (int x = 1; x <= msgCount; x++) {
                try {
                    js.publish(SUBJECT, ("Data # " + x).getBytes(StandardCharsets.US_ASCII));
                } catch (IOException | JetStreamApiException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static class JsQueueSubscriber implements Runnable {
        int msgCount;
        JetStream js;
        JetStreamSubscription sub;
        AtomicInteger allReceived;
        AtomicInteger thisReceived;

        public JsQueueSubscriber(int msgCount, JetStream js, JetStreamSubscription sub, AtomicInteger allReceived) {
            this.msgCount = msgCount;
            this.js = js;
            this.sub = sub;
            this.allReceived = allReceived;
            this.thisReceived = new AtomicInteger();
        }

        @Override
        public void run() {
            while (allReceived.get() < msgCount) {
                try {
                    Message msg = sub.nextMessage(Duration.ofMillis(500));
                    while (msg != null) {
                        thisReceived.incrementAndGet();
                        allReceived.incrementAndGet();
                        msg.ack();
                        msg = sub.nextMessage(Duration.ofMillis(500));
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
