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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPushHandlerTests extends JetStreamTestBase {

    @Test
    public void testHandlerSub() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // publish some messages
            publish(js, SUBJECT, 10);

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher(null);

            CountDownLatch msgLatch = new CountDownLatch(10);
            AtomicInteger received = new AtomicInteger();

            // create our message handler.
            MessageHandler handler = (Message msg) -> {
                received.incrementAndGet();

                if (msg.isJetStream()) {
                    msg.ack();
                }

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
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // publish some messages
            publish(js, SUBJECT, 10);

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher(null);

            // auto ack true
            CountDownLatch msgLatch1 = new CountDownLatch(10);
            AtomicInteger received1 = new AtomicInteger();

            // create our message handler.
            MessageHandler handler1 = (Message msg) -> {
                received1.incrementAndGet();
                msgLatch1.countDown();
            };

            // Subscribe using the handler
            PushSubscribeOptions pso1 = PushSubscribeOptions.builder().durable(durable(1)).build();
            js.subscribe(SUBJECT, dispatcher, handler1, true, pso1);

            // Wait for messages to arrive using the countdown latch.
            msgLatch1.await();

            assertEquals(10, received1.get());

            JetStreamSubscription sub = js.subscribe(SUBJECT, pso1);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

            // auto ack false
            CountDownLatch msgLatch2 = new CountDownLatch(10);
            AtomicInteger received2 = new AtomicInteger();

            // create our message handler.
            MessageHandler handler2 = (Message msg) -> {
                received2.incrementAndGet();
                msgLatch2.countDown();
            };

            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackWait(Duration.ofSeconds(1)).build();
            PushSubscribeOptions pso2 = PushSubscribeOptions.builder().durable(durable(2)).configuration(cc).build();
            js.subscribe(SUBJECT, dispatcher, handler2, false, pso2);

            msgLatch2.await();

            assertEquals(10, received2.get());

            sub = js.subscribe(SUBJECT, pso2);
            assertNotNull(sub.nextMessage(Duration.ofSeconds(5)));
        });
    }
}
