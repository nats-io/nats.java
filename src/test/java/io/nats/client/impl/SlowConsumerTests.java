// Copyright 2015-2018 The NATS Authors
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

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

import org.junit.Test;

import io.nats.client.Consumer;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.Subscription;
import io.nats.client.TestHandler;

public class SlowConsumerTests {

    @Test
    public void testDefaultPendingLimits() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            
            Subscription sub = nc.subscribe("subject");
            Dispatcher d = nc.createDispatcher((Message m) -> {});

            assertEquals(sub.getPendingMessageLimit(), Consumer.DEFAULT_MAX_MESSAGES);
            assertEquals(sub.getPendingByteLimit(), Consumer.DEFAULT_MAX_BYTES);

            assertEquals(d.getPendingMessageLimit(), Consumer.DEFAULT_MAX_MESSAGES);
            assertEquals(d.getPendingByteLimit(), Consumer.DEFAULT_MAX_BYTES);
        }
    }

    @Test
    public void testSlowSubscriberByMessages() throws Exception {

        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            
            Subscription sub = nc.subscribe("subject");
            sub.setPendingLimits(1, -1);

            assertEquals(1, sub.getPendingMessageLimit());
            assertEquals(-1, sub.getPendingByteLimit());
            assertEquals(0, sub.getDroppedCount());

            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(3, sub.getDroppedCount());
            assertEquals(1, sub.getPendingMessageCount());
            assertEquals(19, sub.getPendingByteCount()); // "msg 1 subject 0" + crlf + crlf

            sub.clearDroppedCount();
            
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, sub.getDroppedCount());
            assertEquals(1, sub.getPendingMessageCount());
        }
    }

    @Test
    public void testSlowSubscriberByBytes() throws Exception {

        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            
            Subscription sub = nc.subscribe("subject");
            sub.setPendingLimits(-1, 10); // will take the first, not the second

            assertEquals(10, sub.getPendingByteLimit());
            assertEquals(-1, sub.getPendingMessageLimit());
            assertEquals(0, sub.getDroppedCount());

            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, sub.getDroppedCount());
            assertEquals(1, sub.getPendingMessageCount());
            assertEquals(19, sub.getPendingByteCount()); // "msg 1 subject 0" + crlf + crlf

            sub.clearDroppedCount();
            
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, sub.getDroppedCount());
            assertEquals(1, sub.getPendingMessageCount());
        }
    }

    @Test
    public void testSlowSDispatcherByMessages() throws Exception {

        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            
            final CompletableFuture<Void> ok = new CompletableFuture<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                ok.complete(null);
                Thread.sleep(5 * 60 * 1000); // will wait until interrupted
            });

            d.setPendingLimits(1, -1);
            d.subscribe("subject");

            assertEquals(1, d.getPendingMessageLimit());
            assertEquals(-1, d.getPendingByteLimit());
            assertEquals(0, d.getDroppedCount());

            nc.publish("subject", null);

            ok.get(1000,TimeUnit.MILLISECONDS); // make sure we got the first one
            
            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(1000));

            assertEquals(1, d.getDroppedCount());
            assertEquals(1, d.getPendingMessageCount());
            assertEquals(19, d.getPendingByteCount()); // "msg 1 subject 0" + crlf + crlf

            d.clearDroppedCount();
            
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, d.getDroppedCount());
            assertEquals(1, d.getPendingMessageCount());
        }
    }

    @Test
    public void testSlowSDispatcherByBytes() throws Exception {

        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            
            final CompletableFuture<Void> ok = new CompletableFuture<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                ok.complete(null);
                Thread.sleep(5 * 60 * 1000); // will wait until interrupted
            });

            d.setPendingLimits(-1, 10);
            d.subscribe("subject");

            assertEquals(-1, d.getPendingMessageLimit());
            assertEquals(10, d.getPendingByteLimit());
            assertEquals(0, d.getDroppedCount());

            nc.publish("subject", null);

            ok.get(1000,TimeUnit.MILLISECONDS); // make sure we got the first one
            
            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, d.getDroppedCount());
            assertEquals(1, d.getPendingMessageCount());
            assertEquals(19, d.getPendingByteCount()); // "msg 1 subject 0" + crlf + crlf

            d.clearDroppedCount();
            
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, d.getDroppedCount());
            assertEquals(1, d.getPendingMessageCount());
        }
    }

    @Test
    public void testSlowSubscriberNotification() throws Exception {
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(new Options.Builder().
                                                                    server(ts.getURI()).errorListener(handler).build())) {
            
            Subscription sub = nc.subscribe("subject");
            sub.setPendingLimits(1, -1);

            Future<Boolean> waitForSlow = handler.waitForSlow();

            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(5000));

            // Notification is in another thread, wait for it, or fail
            waitForSlow.get(1000, TimeUnit.MILLISECONDS);

            List<Consumer> slow = handler.getSlowConsumers();
            assertEquals(1, slow.size()); // should only appear once
            assertEquals(sub, slow.get(0));
            slow.clear();
            
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(1000));

            assertEquals(0, slow.size()); // no renotifiy

            waitForSlow = handler.waitForSlow();
            // Clear the queue, we shoudl become a non-slow consumer
            sub.nextMessage(Duration.ofMillis(1000)); // only 1 to get

            // Notification again on 2nd message
            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.flush(Duration.ofMillis(1000));

            waitForSlow.get(1000, TimeUnit.MILLISECONDS);

            assertEquals(1, slow.size()); // should only appear once
            assertEquals(sub, slow.get(0));
        }
    }
}