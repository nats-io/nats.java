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

package io.nats.client;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.utils.ConnectionUtils.standardConnect;
import static io.nats.client.utils.OptionsUtils.options;
import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public class SubscriberTests {

    @Test
    public void testCreateInbox() throws Exception {
        runInShared(nc -> {
            HashSet<String> check = new HashSet<>();
            for (int i=0; i < 100; i++) {
                String inbox = nc.createInbox();
                assertFalse(check.contains(inbox));
                check.add(inbox);
            }
        });
    }

    @Test
    public void testSingleMessage() throws Exception {
        runInShared(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);
            nc.publish(subject, new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500));

            assertTrue(sub.isActive());
            assertEquals(subject, msg.getSubject());
            assertEquals(sub, msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(16, msg.getData().length);
        });
    }

    @Test
    public void testMessageFromSubscriptionContainsConnection() throws Exception {
        runInShared(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);
            nc.publish(subject, new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500));

            assertTrue(sub.isActive());
            assertEquals(subject, msg.getSubject());
            assertEquals(sub, msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(16, msg.getData().length);
            assertSame(msg.getConnection(), nc);
        });
    }

    @Test
    public void testTabInProtocolLine() throws Exception {
        CompletableFuture<Boolean> gotSub = new CompletableFuture<>();
        CompletableFuture<Boolean> sendMsg = new CompletableFuture<>();

        NatsServerProtocolMock.Customizer receiveMessageCustomizer = (ts, r,w) -> {
            String subLine;
            
            // System.out.println("*** Mock Server @" + ts.getPort() + " waiting for SUB ...");
            try {
                subLine = r.readLine();
            } catch(Exception e) {
                gotSub.cancel(true);
                return;
            }

            if (subLine.startsWith("SUB")) {
                gotSub.complete(Boolean.TRUE);
            }

            String[] parts = subLine.split("\\s");
            String subject = parts[1];
            int subId = Integer.parseInt(parts[2]);

            try {
                sendMsg.get();
            } catch (Exception e) {
                //keep going
            }

            w.write("MSG\t"+subject+"\t"+subId+"\t0\r\n\r\n");
            w.flush();
        };

        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(receiveMessageCustomizer)) {
            try (Connection nc = standardConnect(options(mockTs))) {
                String subject = random();
                Subscription sub = nc.subscribe(subject);

                gotSub.get();
                sendMsg.complete(Boolean.TRUE);

                Message msg = sub.nextMessage(Duration.ZERO);//Duration.ofMillis(1000));

                assertTrue(sub.isActive());
                assertNotNull(msg);
                assertEquals(subject, msg.getSubject());
                assertEquals(sub, msg.getSubscription());
                assertNull(msg.getReplyTo());
                assertEquals(0, msg.getData().length);
            }
        }
    }

    @Test
    public void testMultiMessage() throws Exception {
        runInShared(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);
            nc.publish(subject, new byte[16]);
            nc.publish(subject, new byte[16]);
            nc.publish(subject, new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500));

            assertEquals(subject, msg.getSubject());
            assertEquals(sub, msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(16, msg.getData().length);
            msg = sub.nextMessage(100); // coverage for nextMessage(millis)
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);
            msg = sub.nextMessage(100); // coverage for nextMessage(millis)
            assertNull(msg);
        });
    }

    @Test
    public void testQueueSubscribers() throws Exception {
        runInShared(nc -> {
            int msgs = 100;
            int received = 0;
            int sub1Count = 0;
            int sub2Count = 0;
            Message msg;

            String subject = random();
            String queue = random();
            Subscription sub1 = nc.subscribe(subject, queue);
            Subscription sub2 = nc.subscribe(subject, queue);

            for (int i = 0; i < msgs; i++) {
                nc.publish(subject, new byte[16]);
            }

            nc.flush(Duration.ofMillis(200));// Get them all to the server

            for (int i = 0; i < msgs; i++) {
                msg = sub1.nextMessage(null);

                if (msg != null) {
                    assertEquals(subject, msg.getSubject());
                    assertNull(msg.getReplyTo());
                    assertEquals(16, msg.getData().length);
                    received++;
                    sub1Count++;
                }
            }

            for (int i = 0; i < msgs; i++) {
                msg = sub2.nextMessage(null);

                if (msg != null) {
                    assertEquals(subject, msg.getSubject());
                    assertNull(msg.getReplyTo());
                    assertEquals(16, msg.getData().length);
                    received++;
                    sub2Count++;
                }
            }

            assertEquals(msgs, received);
            assertEquals(msgs, sub1Count + sub2Count);
        });
    }

    @Test
    public void testUnsubscribe() throws Exception {
        runInShared(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);
            nc.publish(subject, new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500));
            assertNotNull(msg);

            sub.unsubscribe();
            assertFalse(sub.isActive());
            assertThrows(IllegalStateException.class, () -> sub.nextMessage(Duration.ofMillis(500)));
        });
    }

    @Test
    public void testAutoUnsubscribe() throws Exception {
        runInShared(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject).unsubscribe(1);
            nc.publish(subject, new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500)); // should get 1
            assertNotNull(msg);

            assertThrows(IllegalStateException.class, () -> sub.nextMessage(Duration.ofMillis(500)));
        });
    }

    @Test
    public void testMultiAutoUnsubscribe() throws Exception {
        runInShared(nc -> {
            String subject = random();
            int msgCount = 10;
            Subscription sub = nc.subscribe(subject).unsubscribe(msgCount);

            for (int i = 0; i < msgCount; i++) {
                nc.publish(subject, new byte[16]);
            }

            Message msg;
            for (int i = 0; i < msgCount; i++) {
                msg = sub.nextMessage(Duration.ofMillis(500)); // should get 1
                assertNotNull(msg);
            }

            assertThrows(IllegalStateException.class, () -> sub.nextMessage(Duration.ofMillis(500)));
        });
    }

    @Test
    public void testOnlyOneUnsubscribe() throws Exception {
        runInShared(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);
            sub.unsubscribe();
            assertThrows(IllegalStateException.class, sub::unsubscribe);
        });
    }

    @Test
    public void testOnlyOneAutoUnsubscribe() throws Exception {
        runInShared(nc -> {
            String subject = random();

            Subscription sub = nc.subscribe(subject).unsubscribe(1);
            nc.publish(subject, new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500)); // should get 1
            assertNotNull(msg);

            assertThrows(IllegalStateException.class, sub::unsubscribe);
        });
    }

    @Test
    public void testUnsubscribeInAnotherThread() throws Exception {
        runInShared(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);
            new Thread(sub::unsubscribe).start();
            assertThrows(IllegalStateException.class, () -> sub.nextMessage(Duration.ofMillis(5000)));
        });
    }

    @Test
    public void testAutoUnsubAfterMaxIsReached() throws Exception {
        runInShared(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);

            int msgCount = 10;
            for (int i = 0; i < msgCount; i++) {
                nc.publish(subject, new byte[16]);
            }

            nc.flush(Duration.ofMillis(1000)); // Slow things down so we have time to unsub

            for (int i = 0; i < msgCount; i++) {
                sub.nextMessage(null);
            }

            sub.unsubscribe(msgCount); // we already have that many

            assertThrows(IllegalStateException.class, () -> sub.nextMessage(Duration.ofMillis(5000)));
        });
    }

    @Test
    public void testSubscribesThatException() throws Exception {
        // own nc b/c we will close
        runInShared(nc -> {

            // null subject
            //noinspection DataFlowIssue
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe(null));

            // empty subject
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe(""));

            // null queue
            //noinspection DataFlowIssue
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe(random(), null));

            // empty queue
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe(random(), ""));

            // null subject with queue
            //noinspection DataFlowIssue
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe(null, random()));

            // empty subject with queue
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe("", random()));
        });
    }

    @Test
    public void testSubscribesThatExceptionAfterClose() throws Exception {
        // own nc b/c we will close
        runInSharedOwnNc(nc -> {
            Subscription sub = nc.subscribe(random());
            nc.close();

            /// can't subscribe when closed
            assertThrows(IllegalStateException.class, () -> nc.subscribe(random()));

            /// can't unsubscribe when closed
            assertThrows(IllegalStateException.class, sub::unsubscribe);

            /// can't auto unsubscribe when closed
            assertThrows(IllegalStateException.class, () -> sub.unsubscribe(1));
        });
    }

    @Test
    public void testUnsubscribeWhileWaiting() throws Exception {
        runInShared(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);
            nc.flush(Duration.ofMillis(1000));

            new Thread(() -> {
                try {
                    Thread.sleep(100);
                }
                catch (Exception e) { /* ignored */ }
                sub.unsubscribe();
            }).start();

            assertThrows(IllegalStateException.class, () -> sub.nextMessage(Duration.ofMillis(5000)));
        });
    }
}
