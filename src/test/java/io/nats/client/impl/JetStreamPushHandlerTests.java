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

import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JetStreamPushHandlerTests extends JetStreamTestBase {

    @Test
    public void testHandlerSub() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

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

            // publish some messages
            publish(js, SUBJECT, 10);

            // Subscribe using the handler
            js.subscribe(SUBJECT, dispatcher, handler, false);

            // Wait for messages to arrive using the countdown latch. But don't wait forever.
            msgLatch.await(3, TimeUnit.SECONDS);

            assertEquals(10, received.get());
        });
    }
}
