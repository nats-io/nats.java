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

import io.nats.client.Consumer;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SlowConsumerTests extends TestBase {

    @Test
    public void testDefaultPendingLimits() throws Exception {
        runInSharedOwnNc(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);
            Dispatcher d = nc.createDispatcher((Message m) -> {});

            assertEquals(Consumer.DEFAULT_MAX_MESSAGES, sub.getPendingMessageLimit());
            assertEquals(Consumer.DEFAULT_MAX_BYTES, sub.getPendingByteLimit());

            assertEquals(Consumer.DEFAULT_MAX_MESSAGES, d.getPendingMessageLimit());
            assertEquals(Consumer.DEFAULT_MAX_BYTES, d.getPendingByteLimit());
            nc.closeDispatcher(d);
        });
    }

    @Test
    public void testSlowSubscriberByMessages() throws Exception {
        runInSharedOwnNc(nc -> {
            String subject = random();
            int expectedPending = subject.length() + 12;
            Subscription sub = nc.subscribe(subject);
            sub.setPendingLimits(1, -1);

            assertEquals(1, sub.getPendingMessageLimit());
            assertEquals(0, sub.getPendingByteLimit());
            assertEquals(0, sub.getDroppedCount());

            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(3, sub.getDroppedCount());
            assertEquals(1, sub.getPendingMessageCount());
            assertEquals(expectedPending, sub.getPendingByteCount()); // "msg 1 subject 0" + crlf + crlf

            sub.clearDroppedCount();
            
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, sub.getDroppedCount());
            assertEquals(1, sub.getPendingMessageCount());
        });
    }

    @Test
    public void testSlowSubscriberByBytes() throws Exception {
        runInSharedOwnNc(nc -> {
            String subject = random();
            int maxBytes = subject.length() + 3;
            int expectedPending = maxBytes + 9;
            Subscription sub = nc.subscribe(subject);
            sub.setPendingLimits(-1, maxBytes); // will take the first, not the second

            assertEquals(maxBytes, sub.getPendingByteLimit());
            assertEquals(0, sub.getPendingMessageLimit());
            assertEquals(0, sub.getDroppedCount());

            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, sub.getDroppedCount());
            assertEquals(1, sub.getPendingMessageCount());
            assertEquals(expectedPending, sub.getPendingByteCount()); // "msg 1 subject 0" + crlf + crlf

            sub.clearDroppedCount();
            
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, sub.getDroppedCount());
            assertEquals(1, sub.getPendingMessageCount());
        });
    }

    @Test
    public void testSlowSDispatcherByMessages() throws Exception {
        runInSharedOwnNc(nc -> {
            String subject = random();
            int expectedPending = subject.length() + 12;

            final CompletableFuture<Void> ok = new CompletableFuture<>();
            Dispatcher d = nc.createDispatcher(msg -> {
                ok.complete(null);
                Thread.sleep(5 * 60 * 1000); // will wait until interrupted
            });

            d.setPendingLimits(1, -1);
            d.subscribe(subject);

            assertEquals(1, d.getPendingMessageLimit());
            assertEquals(0, d.getPendingByteLimit());
            assertEquals(0, d.getDroppedCount());

            nc.publish(subject, null);

            ok.get(1000,TimeUnit.MILLISECONDS); // make sure we got the first one
            
            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(1000));

            assertEquals(1, d.getDroppedCount());
            assertEquals(1, d.getPendingMessageCount());
            assertEquals(expectedPending, d.getPendingByteCount()); // "msg 1 subject 0" + crlf + crlf

            d.clearDroppedCount();
            
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, d.getDroppedCount());
            assertEquals(1, d.getPendingMessageCount());
        });
    }

    @Test
    public void testSlowSDispatcherByBytes() throws Exception {
        runInSharedOwnNc(nc -> {
            String subject = random();
            int maxBytes = subject.length() + 3;
            int expectedPending = maxBytes + 9;
            final CompletableFuture<Void> ok = new CompletableFuture<>();
            Dispatcher d = nc.createDispatcher(msg -> {
                ok.complete(null);
                Thread.sleep(5 * 60 * 1000); // will wait until interrupted
            });

            d.setPendingLimits(-1, maxBytes);
            d.subscribe(subject);

            assertEquals(0, d.getPendingMessageLimit());
            assertEquals(maxBytes, d.getPendingByteLimit());
            assertEquals(0, d.getDroppedCount());

            nc.publish(subject, null);

            ok.get(1000,TimeUnit.MILLISECONDS); // make sure we got the first one
            
            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, d.getDroppedCount());
            assertEquals(1, d.getPendingMessageCount());
            assertEquals(expectedPending, d.getPendingByteCount()); // "msg 1 subject 0" + crlf + crlf

            d.clearDroppedCount();
            
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(5000));

            assertEquals(1, d.getDroppedCount());
            assertEquals(1, d.getPendingMessageCount());
        });
    }

    @Test
    public void testSlowSubscriberNotification() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        runInSharedOwnNc(listener, nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);
            sub.setPendingLimits(1, -1);

            Future<Boolean> waitForSlow = listener.waitForSlow();

            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(5000));

            // Notification is in another thread, wait for it, or fail
            waitForSlow.get(3000, TimeUnit.MILLISECONDS);

            List<Consumer> slow = listener.getSlowConsumers();
            assertEquals(1, slow.size()); // should only appear once
            assertEquals(sub, slow.get(0));
            slow.clear();
            
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(1000));

            assertEquals(0, slow.size()); // no renotify

            waitForSlow = listener.waitForSlow();
            // Clear the queue, we should become a non-slow consumer
            sub.nextMessage(Duration.ofMillis(1000)); // only 1 to get

            // Notification again on 2nd message
            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.flush(Duration.ofMillis(1000));

            waitForSlow.get(3000, TimeUnit.MILLISECONDS);

            assertEquals(1, slow.size()); // should only appear once
            assertEquals(sub, slow.get(0));
        });
    }
}