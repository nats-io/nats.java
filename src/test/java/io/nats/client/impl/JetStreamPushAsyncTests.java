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
import io.nats.client.api.ConsumerInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPushAsyncTests extends JetStreamTestBase {

    @Test
    public void testHandlerSub() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            createDefaultTestStream(nc);

            // publish some messages
            jsPublish(js, SUBJECT, 10);

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            CountDownLatch msgLatch = new CountDownLatch(10);
            AtomicInteger received = new AtomicInteger();

            // create our message handler.
            MessageHandler handler = (Message msg) -> {
                received.incrementAndGet();
                msg.ack();
                msgLatch.countDown();
            };

            // Subscribe using the handler
            js.subscribe(SUBJECT, dispatcher, handler, false);

            // Wait for messages to arrive using the countdown latch.
            msgLatch.await();

            assertEquals(10, received.get());
        });
    }

    @Test
    public void testHandlerAutoAck() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            createDefaultTestStream(nc);

            // publish some messages
            jsPublish(js, SUBJECT, 10);

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            // 1. auto ack true
            CountDownLatch msgLatch1 = new CountDownLatch(10);
            AtomicInteger received1 = new AtomicInteger();

            // create our message handler, does not ack
            MessageHandler handler1 = (Message msg) -> {
                received1.incrementAndGet();
                msgLatch1.countDown();
            };

            // subscribe using the handler, auto ack true
            PushSubscribeOptions pso1 = PushSubscribeOptions.builder().durable(durable(1)).build();
            JetStreamSubscription sub  = js.subscribe(SUBJECT, dispatcher, handler1, true, pso1);

            // wait for messages to arrive using the countdown latch.
            msgLatch1.await();

            assertEquals(10, received1.get());

            // check that all the messages were read by the durable
            dispatcher.unsubscribe(sub);
            sub = js.subscribe(SUBJECT, pso1);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

            // 2. auto ack false
            CountDownLatch msgLatch2 = new CountDownLatch(10);
            AtomicInteger received2 = new AtomicInteger();

            // create our message handler, also does not ack
            MessageHandler handler2 = (Message msg) -> {
                received2.incrementAndGet();
                msgLatch2.countDown();
            };

            // subscribe using the handler, auto ack false
            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackWait(Duration.ofMillis(500)).build();
            PushSubscribeOptions pso2 = PushSubscribeOptions.builder().durable(durable(2)).configuration(cc).build();
            sub = js.subscribe(SUBJECT, dispatcher, handler2, false, pso2);

            // wait for messages to arrive using the countdown latch.
            msgLatch2.await();
            assertEquals(10, received2.get());

            ConsumerInfo ci = sub.getConsumerInfo();
            assertEquals(10, ci.getNumAckPending());

            sleep(1000); // just give it time for the server to realize the messages are not ack'ed

            dispatcher.unsubscribe(sub);
            sub = js.subscribe(SUBJECT, pso2);
            List<Message> list = readMessagesAck(sub, false);
            assertEquals(10, list.size());
        });
    }

    @Test
    public void testCantNextMessageOnAsyncPushSub() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            createDefaultTestStream(nc);

            JetStreamSubscription sub = js.subscribe(SUBJECT, nc.createDispatcher(), msg -> {}, false);

            // this should exception, can't next message on an async push sub
            assertThrows(IllegalStateException.class, () -> sub.nextMessage(Duration.ofMillis(1000)));
            assertThrows(IllegalStateException.class, () -> sub.nextMessage(1000));
        });
    }

    @Test
    public void testPushAsyncFlowControl() throws Exception {
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

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            CountDownLatch msgLatch = new CountDownLatch(MSG_COUNT);
            AtomicInteger count = new AtomicInteger();
            AtomicReference<Set<String>> set = new AtomicReference<>(new HashSet<>());

            // create our message handler.
            MessageHandler handler = (Message msg) -> {
                String id = new String(Arrays.copyOf(msg.getData(), 6));
                if (set.get().add(id)) {
                    count.incrementAndGet();
                }
                msg.ack();
                msgLatch.countDown();
            };

            ConsumerConfiguration cc = ConsumerConfiguration.builder().flowControl(1000).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            js.subscribe(SUBJECT, dispatcher, handler, false, pso);

            // Wait for messages to arrive using the countdown latch.
            msgLatch.await();

            assertEquals(MSG_COUNT, count.get());
            assertTrue(fcps.get() > 0);
        });
    }

    @Test
    public void testDontAutoAckIfUserAcks() throws Exception {
        String mockAckReply = "mock-ack-reply.";

        runInJsServer(nc -> {
            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT, mockAckReply + "*");

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // publish a message
            jsPublish(js, SUBJECT, 2);

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            CountDownLatch msgLatch = new CountDownLatch(2);

            AtomicBoolean flag = new AtomicBoolean(true);

            // create our message handler, does not ack
            MessageHandler handler = (Message msg) -> {
                NatsJetStreamMessage m = (NatsJetStreamMessage)msg;
                if (flag.get()) {
                    m.replyTo = mockAckReply + "user";
                    m.ack();
                    flag.set(false);
                }
                m.replyTo = mockAckReply + "system";
                msgLatch.countDown();
            };

            // subscribe using the handler, auto ack true
            js.subscribe(SUBJECT, dispatcher, handler, true);

            // wait for messages to arrive using the countdown latch.
            msgLatch.await();

            JetStreamSubscription sub = js.subscribe(mockAckReply + "*");
            Message msg = sub.nextMessage(1000);
            assertEquals(mockAckReply + "user", msg.getSubject());
            msg = sub.nextMessage(1000);
            assertEquals(mockAckReply + "system", msg.getSubject());
        });
    }
}
