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
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PriorityPolicy;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.Listener;
import io.nats.client.support.ListenerStatusType;
import io.nats.client.utils.ConnectionUtils;
import io.nats.client.utils.VersionUtils;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.client.api.ConsumerConfiguration.builder;
import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.ListenerStatusType.PullError;
import static io.nats.client.support.ListenerStatusType.PullWarning;
import static io.nats.client.support.NatsJetStreamConstants.NATS_PIN_ID_HDR;
import static io.nats.client.support.Status.*;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

@Isolated
public class JetStreamPullTests extends JetStreamTestBase {

    static Connection conflictNc;
    static Listener conflictListener;

    @AfterAll
    public static void afterAll() {
        testBaseAfterAll();
        if (conflictNc != null) {
            try {
                conflictNc.close();
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    public void testFetch() throws Exception {
        runInShared((nc, ctx) -> {
            long fetchMs = 3000;
            Duration fetchDur = Duration.ofMillis(fetchMs);
            Duration ackWaitDur = Duration.ofMillis(fetchMs * 2);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackWait(ackWaitDur)
                .build();

            PullSubscribeOptions options = PullSubscribeOptions.builder()
                .durable(ctx.consumerName())
                .configuration(cc)
                .build();

            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), options);
            assertSubscription(sub, ctx.stream, ctx.consumerName(), null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            List<Message> messages = sub.fetch(10, fetchDur);
            validateRead(0, messages.size());
            messages.forEach(Message::ack);
            sleep(ackWaitDur.toMillis()); // let the pull expire

            jsPublish(ctx.js, ctx.subject(), "A", 10);
            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(ctx.js, ctx.subject(), "B", 20);
            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(ctx.js, ctx.subject(), "C", 5);
            messages = sub.fetch(10, fetchDur);
            validateRead(5, messages.size());
            messages.forEach(Message::ack);
            sleep(fetchMs); // let the pull expire

            jsPublish(ctx.js, ctx.subject(), "D", 15);
            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            messages = sub.fetch(10, fetchDur);
            validateRead(5, messages.size());
            messages.forEach(Message::ack);

            jsPublish(ctx.js, ctx.subject(), "E", 10);
            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            sleep(ackWaitDur.toMillis()); // let the acks wait expire, pull will also expire it's shorter

            // message were not ack'ed
            messages = sub.fetch(10, fetchDur);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            assertThrows(IllegalArgumentException.class, () -> sub.fetch(10, null));
            assertThrows(IllegalArgumentException.class, () -> sub.fetch(10, Duration.ofSeconds(-1)));
        });
    }

    @Test
    public void testIterate() throws Exception {
        runInShared((nc, ctx) -> {
            long fetchMs = 5000;
            Duration fetchDur = Duration.ofMillis(fetchMs);
            Duration ackWaitDur = Duration.ofMillis(fetchMs * 2);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackWait(ackWaitDur)
                .build();

            PullSubscribeOptions options = PullSubscribeOptions.builder()
                .durable(ctx.consumerName())
                .configuration(cc)
                .build();

            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), options);
            assertSubscription(sub, ctx.stream, ctx.consumerName(), null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            Iterator<Message> iterator = sub.iterate(10, fetchDur);
            List<Message> messages = readMessages(iterator);
            validateRead(0, messages.size());
            messages.forEach(Message::ack);

            jsPublish(ctx.js, ctx.subject(), "A", 10);
            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(ctx.js, ctx.subject(), "B", 20);
            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(ctx.js, ctx.subject(), "C", 5);
            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(5, messages.size());
            messages.forEach(Message::ack);
            sleep(fetchMs); // give time for the pull to expire

            jsPublish(ctx.js, ctx.subject(), "D", 15);
            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(5, messages.size());
            messages.forEach(Message::ack);
            sleep(fetchMs); // give time for the pull to expire

            jsPublish(ctx.js, ctx.subject(), "E", 10);
            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            sleep(ackWaitDur.toMillis()); // give time for the pull and the ack wait to expire

            iterator = sub.iterate(10, fetchDur);
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(ctx.js, ctx.subject(), "F", 1);
            iterator = sub.iterate(1, fetchDur);
            //noinspection ResultOfMethodCallIgnored
            iterator.hasNext(); // calling hasNext twice in a row is for coverage
            //noinspection ResultOfMethodCallIgnored
            iterator.hasNext(); // calling hasNext twice in a row is for coverage
        });
    }

    @Test
    public void testBasic() throws Exception {
        runInShared((nc, ctx) -> {
            // Build our subscription options.
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(ctx.consumerName()).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), options);
            assertSubscription(sub, ctx.stream, ctx.consumerName(), null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish some amount of messages, but not entire pull size
            jsPublish(ctx.js, ctx.subject(), "A", 4);

            // start the pull
            sub.pull(10);

            // read what is available, expect 4
            List<Message> messages = readMessagesAck(sub);
            int total = messages.size();
            validateRedAndTotal(4, messages.size(), 4, total);

            // publish some more covering our initial pull and more
            jsPublish(ctx.js, ctx.subject(), "B", 10);

            // read what is available, expect 6 more
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(6, messages.size(), 10, total);

            // read what is available, should be zero since we didn't re-pull
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 10, total);

            // re-issue the pull
            sub.pull(PullRequestOptions.builder(10).build()); // coverage of the build api

            // read what is available, should be 4 left over
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(4, messages.size(), 14, total);

            // publish some more
            jsPublish(ctx.js, ctx.subject(), "C", 10);

            // read what is available, should be 6 since we didn't finish the last batch
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(6, messages.size(), 20, total);

            // re-issue the pull, but a smaller amount
            sub.pull(2);

            // read what is available, should be 2 since we changed the pull size
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(2, messages.size(),22, total);

            // re-issue the pull, since we got the full batch size
            sub.pull(2);

            // read what is available, should be 2
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(2, messages.size(), 24, total);

            // re-issue the pull, any amount there are no messages
            sub.pull(1);

            // read what is available, there are none
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 24, total);

            // publish some more to test null timeout
            jsPublish(ctx.js, ctx.subject(), "D", 10);
            sub = ctx.js.subscribe(ctx.subject(), PullSubscribeOptions.builder().durable(random()).build());
            sub.pull(10);
            sleep(500);
            messages = readMessagesAck(sub, null);
            validateRedAndTotal(10, messages.size(), 10, messages.size());

            // publish some more to test never timeout
            jsPublish(ctx.js, ctx.subject(), "E", 10);
            sub = ctx.js.subscribe(ctx.subject(), PullSubscribeOptions.builder().durable(random()).build());
            sub.pull(10);
            sleep(500);
            messages = readMessagesAck(sub, Duration.ZERO, 10);
            validateRedAndTotal(10, messages.size(), 10, messages.size());
        });
    }

    @Test
    public void testNoWait() throws Exception {
        runInShared((nc, ctx) -> {
            // Build our subscription options.
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(ctx.consumerName()).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), options);
            assertSubscription(sub, ctx.stream, ctx.consumerName(), null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish 10 messages
            // no wait, batch size 10, there are 10 messages, we will read them all and not trip nowait
            jsPublish(ctx.js, ctx.subject(), "A", 10);
            sub.pullNoWait(10);
            List<Message> messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            // publish 20 messages
            // no wait, batch size 10, there are 20 messages, we will read 10
            jsPublish(ctx.js, ctx.subject(), "B", 20);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            // there are still ten messages
            // no wait, batch size 10, there are 20 messages, we will read 10
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            // publish 5 messages
            // no wait, batch size 10, there are 5 messages, we WILL trip nowait
            jsPublish(ctx.js, ctx.subject(), "C", 5);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());

            // publish 12 messages
            // no wait, batch size 10, there are more than batch messages we will read 10
            jsPublish(ctx.js, ctx.subject(), "D", 12);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            // 2 messages left
            // no wait, less than batch size will trip nowait
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(2, messages.size());

            // this is just coverage of the pullNoWait api + expires, not really validating server functionality
            // publish 12 messages
            // no wait, batch size 10, there are more than batch messages we will read 10
            jsPublish(ctx.js, ctx.subject(), "E", 12);
            sub.pullNoWait(10, 10000);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            // 2 messages left
            // no wait, less than batch size will trip nowait
            sub.pullNoWait(10, Duration.ofMillis(1000));
            messages = readMessagesAck(sub);
            assertEquals(2, messages.size());
        });
    }

    @Test
    public void testPullExpires() throws Exception {
        runInShared((nc, ctx) -> {
            // Build our subscription options.
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(ctx.consumerName()).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), options);
            assertSubscription(sub, ctx.stream, ctx.consumerName(), null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            long expires = 500; // millis

            // publish 10 messages
            jsPublish(ctx.js, ctx.subject(), "A", 5);
            sub.pullExpiresIn(10, Duration.ofMillis(expires)); // using Duration version here
            List<Message> messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(ctx.js, ctx.subject(), "B", 10);
            sub.pullExpiresIn(10, Duration.ofMillis(expires)); // using Duration version here
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            sleep(expires); // make sure the pull actually expires

            jsPublish(ctx.js, ctx.subject(), "C", 5);
            sub.pullExpiresIn(10, Duration.ofMillis(expires)); // using Duration version here
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(ctx.js, ctx.subject(), "D", 10);
            sub.pull(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            jsPublish(ctx.js, ctx.subject(), "E", 5);
            sub.pullExpiresIn(10, expires); // using millis version here
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(ctx.js, ctx.subject(), "F", 10);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            jsPublish(ctx.js, ctx.subject(), "G", 5);
            sub.pullExpiresIn(10, expires); // using millis version here
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(ctx.js, ctx.subject(), "H", 10);
            messages = sub.fetch(10, expires);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            jsPublish(ctx.js, ctx.subject(), "I", 5);
            sub.pullExpiresIn(10, expires);
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(ctx.js, ctx.subject(), "J", 10);
            Iterator<Message> i = sub.iterate(10, expires);
            int count = 0;
            while (i.hasNext()) {
                assertIsJetStream(i.next());
                ++count;
            }
            assertEquals(10, count);

            assertThrows(IllegalArgumentException.class, () -> sub.pullExpiresIn(10, null));
            assertThrows(IllegalArgumentException.class, () -> sub.pullExpiresIn(10, Duration.ofSeconds(-1)));
            assertThrows(IllegalArgumentException.class, () -> sub.pullExpiresIn(10, -1000));
        });
    }

    @Test
    public void testAckNak() throws Exception {
        runInShared((nc, ctx) -> {
            PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(random()).build();
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // NAK
            jsPublish(ctx.js, ctx.subject(), "NAK", 1);

            sub.pull(1);

            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("NAK1", data);
            message.nak();

            sub.pull(1);
            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK1", data);
            message.ack();

            sub.pull(1);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testAckTerm() throws Exception {
        runInShared((nc, ctx) -> {
            PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(random()).build();
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // TERM
            jsPublish(ctx.js, ctx.subject(), "TERM", 1);

            sub.pull(1);
            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("TERM1", data);
            message.term();

            sub.pull(1);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testAckReplySyncCoverage() throws Exception {
        runInShared((nc, ctx) -> {
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject());
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            jsPublish(ctx.js, ctx.subject(), "COVERAGE", 1);

            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);

            NatsJetStreamMessage njsMsg = (NatsJetStreamMessage)message;

            njsMsg.replyTo = "$tsc.js.ACK.stream.LS0k4eeN.1.1.1.1627472530542070600.0";

            assertThrows(TimeoutException.class, () -> njsMsg.ackSync(Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testAckWaitTimeout() throws Exception {
        runInShared((nc, ctx) -> {
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackWait(1500)
                .build();
            PullSubscribeOptions pso = PullSubscribeOptions.builder()
                .durable(ctx.consumerName())
                .configuration(cc)
                .build();

            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // Ack Wait timeout
            jsPublish(ctx.js, ctx.subject(), "WAIT", 2);

            sub.pull(2);
            Message m = sub.nextMessage(1000);
            assertNotNull(m);
            assertEquals("WAIT1", new String(m.getData()));

            m = sub.nextMessage(1000);
            assertNotNull(m);
            assertEquals("WAIT2", new String(m.getData()));

            sleep(2000);

            sub.pull(2);
            m = sub.nextMessage(1000);
            assertNotNull(m);
            assertEquals("WAIT1", new String(m.getData()));
            m.ack();

            m = sub.nextMessage(1000);
            assertNotNull(m);
            assertEquals("WAIT2", new String(m.getData()));
            m.ack();

            sub.pull(2);
            m = sub.nextMessage(1000);
            assertNull(m);
        });
    }

    @Test
    public void testDurable() throws Exception {
        runInShared((nc, ctx) -> {
            String durable = random();

            // Build our subscription options normally
            PullSubscribeOptions options1 = PullSubscribeOptions.builder().durable(durable).build();
            _testDurableOrNamed(ctx.js, ctx.subject(), () -> ctx.js.subscribe(ctx.subject(), options1));

            // bind long form
            PullSubscribeOptions options2 = PullSubscribeOptions.builder()
                .stream(ctx.stream)
                .durable(durable)
                .bind(true)
                .build();
            _testDurableOrNamed(ctx.js, ctx.subject(), () -> ctx.js.subscribe(null, options2));

            // fast bind long form
            PullSubscribeOptions options3 = PullSubscribeOptions.builder()
                .stream(ctx.stream)
                .durable(durable)
                .fastBind(true)
                .build();
            _testDurableOrNamed(ctx.js, ctx.subject(), () -> ctx.js.subscribe(null, options3));

            // bind short form
            PullSubscribeOptions options4 = PullSubscribeOptions.bind(ctx.stream, durable);
            _testDurableOrNamed(ctx.js, ctx.subject(), () -> ctx.js.subscribe(null, options4));

            // fast bind short form
            PullSubscribeOptions options5 = PullSubscribeOptions.fastBind(ctx.stream, durable);
            _testDurableOrNamed(ctx.js, ctx.subject(), () -> ctx.js.subscribe(null, options5));
        });
    }

    @Test
    public void testNamed() throws Exception {
        runInShared((nc, ctx) -> {
            String name = random();

            ctx.jsm.addOrUpdateConsumer(ctx.stream, ConsumerConfiguration.builder()
                .name(name)
                .inactiveThreshold(10_000)
                .build());

            // bind long form
            PullSubscribeOptions options2 = PullSubscribeOptions.builder()
                .stream(ctx.stream)
                .name(name)
                .bind(true)
                .build();
            _testDurableOrNamed(ctx.js, ctx.subject(), () -> ctx.js.subscribe(null, options2));

            // fast bind long form
            PullSubscribeOptions options3 = PullSubscribeOptions.builder()
                .stream(ctx.stream)
                .name(name)
                .fastBind(true)
                .build();
            _testDurableOrNamed(ctx.js, ctx.subject(), () -> ctx.js.subscribe(null, options3));

            // bind short form
            PullSubscribeOptions options4 = PullSubscribeOptions.bind(ctx.stream, name);
            _testDurableOrNamed(ctx.js, ctx.subject(), () -> ctx.js.subscribe(null, options4));

            // fast bind short form
            PullSubscribeOptions options5 = PullSubscribeOptions.fastBind(ctx.stream, name);
            _testDurableOrNamed(ctx.js, ctx.subject(), () -> ctx.js.subscribe(null, options5));
        });
    }

    private void _testDurableOrNamed(JetStream js, String subject, SubscriptionSupplier supplier) throws IOException, JetStreamApiException, InterruptedException {
        jsPublish(js, subject, 2);

        JetStreamSubscription sub = supplier.get();

        // start the pull
        sub.pullNoWait(4);

        List<Message> messages = readMessagesAck(sub);
        validateRedAndTotal(2, messages.size(), 2, 2);

        sub.unsubscribe();
    }

    private interface SubscriptionSupplier {
        JetStreamSubscription get() throws IOException, JetStreamApiException;
    }

    @Test
    public void testPullRequestOptionsBuilder() {
        assertThrows(IllegalArgumentException.class, () -> PullRequestOptions.builder(0).build());
        assertThrows(IllegalArgumentException.class, () -> PullRequestOptions.builder(-1).build());
        assertThrows(IllegalArgumentException.class, () -> PullRequestOptions.builder(1).idleHeartbeat(1).build());
        assertThrows(IllegalArgumentException.class, () -> PullRequestOptions.builder(1).noWait().idleHeartbeat(1).build());
        assertThrows(IllegalArgumentException.class, () -> PullRequestOptions.builder(1).expiresIn(30000).idleHeartbeat(15001).build());

        PullRequestOptions pro = PullRequestOptions.builder(11).build();
        assertEquals(11, pro.getBatchSize());
        assertEquals(0, pro.getMaxBytes());
        assertNull(pro.getExpiresIn());
        assertNull(pro.getIdleHeartbeat());
        assertFalse(pro.isNoWait());
        assertNull(pro.getGroup());
        assertEquals(-1, pro.getMinPending());
        assertEquals(-1, pro.getMinAckPending());

        pro = PullRequestOptions.noWait(21).build();
        assertEquals(21, pro.getBatchSize());
        assertEquals(0, pro.getMaxBytes());
        assertNull(pro.getExpiresIn());
        assertNull(pro.getIdleHeartbeat());
        assertTrue(pro.isNoWait());

        pro = PullRequestOptions.builder(31)
            .maxBytes(32)
            .expiresIn(33)
            .idleHeartbeat(16)
            .noWait()
            .build();
        assertEquals(31, pro.getBatchSize());
        assertEquals(32, pro.getMaxBytes());
        assertEquals(33, pro.getExpiresIn().toMillis());
        assertEquals(16, pro.getIdleHeartbeat().toMillis());
        assertTrue(pro.isNoWait());

        pro = PullRequestOptions.builder(41)
            .expiresIn(Duration.ofMillis(43))
            .idleHeartbeat(Duration.ofMillis(21))
            .noWait(false) // just coverage of this method
            .build();
        assertEquals(41, pro.getBatchSize());
        assertEquals(0, pro.getMaxBytes());
        assertEquals(43, pro.getExpiresIn().toMillis());
        assertEquals(21, pro.getIdleHeartbeat().toMillis());
        assertFalse(pro.isNoWait());

        pro = PullRequestOptions.builder(41)
            .group("g")
            .minPending(1)
            .minAckPending(2)
            .build();
        assertEquals("g", pro.getGroup());
        assertEquals(1, pro.getMinPending());
        assertEquals(2, pro.getMinAckPending());
    }

    interface ConflictSetup {
        JetStreamSubscription setup(Connection nc, JetStreamTestingContext ctx) throws Exception;
    }

    interface BuilderCustomizer {
        ConsumerConfiguration.Builder customize(ConsumerConfiguration.Builder b);
    }

    private PullSubscribeOptions makePso(BuilderCustomizer c) {
        return c.customize(ConsumerConfiguration.builder().ackPolicy(AckPolicy.None)).inactiveThreshold(INACTIVE_THRESHOLD).buildPullSubscribeOptions();
    }

    static final long NEXT_MESSAGE_TIMEOUT = 2000;
    static final long INACTIVE_THRESHOLD = 30_000;
    private void _testConflictStatuses(int statusCode, String statusText, ListenerStatusType statusType, ConflictSetup setup) throws Exception {
        runInSharedNamed("conflict", ts -> {
            if (conflictNc == null) {
                conflictListener = new Listener();
                conflictNc = ConnectionUtils.managedConnect(
                    optionsBuilder(ts).errorListener(conflictListener).connectionListener(conflictListener).build());
            }
            else {
                conflictListener.reset();
            }
            try (JetStreamTestingContext tcsCtx = new JetStreamTestingContext(conflictNc, 1)) {
                if (statusType != null) {
                    conflictListener.queueStatus(statusType, statusCode);
                }
                JetStreamSubscription sub = setup.setup(conflictNc, tcsCtx);
                if (sub.getDispatcher() == null) {
                    if (statusType == PullError) {
                        JetStreamStatusException jsse = assertThrows(JetStreamStatusException.class, () -> sub.nextMessage(NEXT_MESSAGE_TIMEOUT));
                        assertEquals(statusCode, jsse.getStatus().getCode());
                        assertEquals(sub.hashCode(), jsse.getSubscription().hashCode());
                        //noinspection deprecation
                        assertTrue(jsse.getDescription().contains(statusText)); // coverage
                    }
                    else {
                        sub.nextMessage(NEXT_MESSAGE_TIMEOUT);
                    }
                }
                if (statusType != null) {
                    conflictListener.validate();
                }
            }
        });
    }

    @Test
    public void testExceededMaxWaitingSync() throws Exception {
        _testConflictStatuses(409, EXCEEDED_MAX_WAITING, PullWarning,
            (nc, ctx) -> {
                PullSubscribeOptions so = makePso(b -> b.maxPullWaiting(1));
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), so);
                sub.pull(1);
                sub.pull(1);
                return sub;
            });
    }


    @Test
    public void testExceededMaxWaitingAsync() throws Exception {
        _testConflictStatuses(409, EXCEEDED_MAX_WAITING, PullWarning,
            (nc, ctx) -> {
                Dispatcher d = nc.createDispatcher();
                PullSubscribeOptions so = makePso(b -> b.maxPullWaiting(1));
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), d, m -> {}, so);
                sub.pull(1);
                sub.pull(1);
                return sub;
            });
    }

    @Test
    public void testExceedsMaxRequestBatchSync() throws Exception {
        _testConflictStatuses(409, EXCEEDED_MAX_REQUEST_BATCH, PullWarning,
            (nc, ctx) -> {
                PullSubscribeOptions so = makePso(b -> b.maxBatch(1));
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), so);
                sub.pull(2);
                return sub;
            });
    }


    @Test
    public void testExceedsMaxRequestBatchAsync() throws Exception {
        _testConflictStatuses(409, EXCEEDED_MAX_REQUEST_BATCH, PullWarning,
            (nc, ctx) -> {
                Dispatcher d = nc.createDispatcher();
                PullSubscribeOptions so = makePso(b -> b.maxBatch(1));
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), d, m -> {}, so);
                sub.pull(2);
                return sub;
            }
        );
    }

    @Test
    public void testMessageSizeExceedsMaxBytesSync() throws Exception {
        _testConflictStatuses(409, MESSAGE_SIZE_EXCEEDS_MAX_BYTES, PullWarning,
            (nc, ctx) -> {
                PullSubscribeOptions so = makePso(b -> b);
                ctx.js.publish(ctx.subject(), new byte[1000]);
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), so);
                sub.pull(PullRequestOptions.builder(1).maxBytes(100).build());
                return sub;
            });
    }


    @Test
    public void testMessageSizeExceedsMaxBytesAsync() throws Exception {
        _testConflictStatuses(409, MESSAGE_SIZE_EXCEEDS_MAX_BYTES, PullWarning,
            (nc, ctx) -> {
                Dispatcher d = nc.createDispatcher();
                PullSubscribeOptions so = makePso(b -> b);
                ctx.js.publish(ctx.subject(), new byte[1000]);
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), d, m -> {}, so);
                sub.pull(PullRequestOptions.builder(1).maxBytes(100).build());
                return sub;
            }
        );
    }

    @Test
    public void testExceedsMaxRequestExpiresSync() throws Exception {
        _testConflictStatuses(409, EXCEEDED_MAX_REQUEST_EXPIRES, PullWarning,
            (nc, ctx) -> {
                PullSubscribeOptions so = makePso(b -> b.maxExpires(1000));
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), so);
                sub.pullExpiresIn(1, 2000);
                return sub;
            });
    }


    @Test
    public void testExceedsMaxRequestExpiresAsync() throws Exception {
        _testConflictStatuses(409, EXCEEDED_MAX_REQUEST_EXPIRES, PullWarning,
            (nc, ctx) -> {
                Dispatcher d = nc.createDispatcher();
                PullSubscribeOptions so = makePso(b -> b.maxExpires(1000));
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), d, m -> {}, so);
                sub.pullExpiresIn(1, 2000);
                return sub;
            }
        );
    }

    @Test
    public void testConsumerIsPushBasedSync() throws Exception {
        _testConflictStatuses(409, CONSUMER_IS_PUSH_BASED, PullError,
            (nc, ctx) -> {
                String dur = random();
                ctx.jsm.addOrUpdateConsumer(ctx.stream, builder().durable(dur).ackPolicy(AckPolicy.None).build());
                PullSubscribeOptions so = PullSubscribeOptions.bind(ctx.stream, dur);
                JetStreamSubscription sub = ctx.js.subscribe(null, so);
                ctx.jsm.deleteConsumer(ctx.stream, dur);
                // consumer with same name but is push now
                ctx.jsm.addOrUpdateConsumer(ctx.stream, builder().durable(dur).deliverSubject(dur).build());
                sub.pull(1);
                return sub;
            });
    }

    @Test
    public void testConsumerIsPushBasedAsync() throws Exception {
        _testConflictStatuses(409, CONSUMER_IS_PUSH_BASED, PullError,
            (nc, ctx) -> {
                String dur = random();
                ctx.jsm.addOrUpdateConsumer(ctx.stream, builder().durable(dur).ackPolicy(AckPolicy.None).build());
                Dispatcher d = nc.createDispatcher();
                PullSubscribeOptions so = PullSubscribeOptions.bind(ctx.stream, dur);
                JetStreamSubscription sub = ctx.js.subscribe(null, d, m -> {}, so);
                ctx.jsm.deleteConsumer(ctx.stream, dur);
                // consumer with same name but is push now
                ctx.jsm.addOrUpdateConsumer(ctx.stream, builder().durable(dur).deliverSubject(dur).build());
                sub.pull(1);
                return sub;
            }
        );
    }

    @Test
    public void testConsumerDeletedSyncSub() throws Exception {
        _testConflictStatuses(409, CONSUMER_DELETED, PullError,
            (nc, ctx) -> {
                String dur = random();
                ctx.jsm.addOrUpdateConsumer(ctx.stream, builder().durable(dur).ackPolicy(AckPolicy.None).build());
                PullSubscribeOptions so = PullSubscribeOptions.bind(ctx.stream, dur);
                JetStreamSubscription sub = ctx.js.subscribe(null, so);
                sub.pullExpiresIn(1, 30000);
                ctx.jsm.deleteConsumer(ctx.stream, dur);
                ctx.js.publish(ctx.subject(), null);
                return sub;
            });
    }

    @Test
    public void testConsumerDeletedAsyncSub() throws Exception {
        _testConflictStatuses(409, CONSUMER_DELETED, PullError,
            // Async
            (nc, ctx) -> {
                String dur = random();
                ctx.jsm.addOrUpdateConsumer(ctx.stream, builder().durable(dur).ackPolicy(AckPolicy.None).build());
                Dispatcher d = nc.createDispatcher();
                PullSubscribeOptions so = PullSubscribeOptions.bind(ctx.stream, dur);
                JetStreamSubscription sub = ctx.js.subscribe(null, d, m -> {}, so);
                sub.pullExpiresIn(1, 30000);
                ctx.jsm.deleteConsumer(ctx.stream, dur);
                ctx.js.publish(ctx.subject(), null);
                return sub;
            }
        );
    }

    @Test
    public void testBadRequestSync() throws Exception {
        _testConflictStatuses(400, BAD_REQUEST, PullError,
            (nc, ctx) -> {
                PullSubscribeOptions so = makePso(b -> b);
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), so);
                sub.pull(new BadPullRequestOptions());
                return sub;
            });
    }

    @Test
    public void testBadRequestAsync() throws Exception {
        _testConflictStatuses(400, BAD_REQUEST, PullError,
            (nc, ctx) -> {
                Dispatcher d = nc.createDispatcher();
                PullSubscribeOptions so = makePso(b -> b);
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), d, m -> {}, so);
                sub.pull(new BadPullRequestOptions());
                return sub;
            });
    }

    @Test
    public void testNotFoundSync() throws Exception {
        _testConflictStatuses(404, NO_MESSAGES, PullWarning,
            (nc, ctx) -> {
                PullSubscribeOptions so = makePso(b -> b);
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), so);
                sub.pullNoWait(1);
                return sub;
            });
    }


    @Test
    public void testNotFoundAsync() throws Exception {
        _testConflictStatuses(404, NO_MESSAGES, PullWarning,
            (nc, ctx) -> {
                Dispatcher d = nc.createDispatcher();
                PullSubscribeOptions so = makePso(b -> b);
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), d, m -> {}, so);
                sub.pullNoWait(1);
                return sub;
            });
    }

    @Test
    public void testExceedsMaxRequestBytes1stMessageSync() throws Exception {
        _testConflictStatuses(409, EXCEEDED_MAX_REQUEST_MAX_BYTES, PullWarning,
            (nc, ctx) -> {
                PullSubscribeOptions so = makePso(b -> b.maxBytes(1));
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), so);
                sub.pull(PullRequestOptions.builder(1).maxBytes(2).build());
                return sub;
            });
    }

    @Test
    public void testExceedsMaxRequestBytes1stMessageAsync() throws Exception {
        _testConflictStatuses(409, EXCEEDED_MAX_REQUEST_MAX_BYTES, PullWarning,
            (nc, ctx) -> {
                Dispatcher d = nc.createDispatcher();
                PullSubscribeOptions so = makePso(b -> b.maxBytes(1));
                JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), d, m -> {}, so);
                sub.pull(PullRequestOptions.builder(1).maxBytes(2).build());
                return sub;
            });
    }

    static class BadPullRequestOptions extends PullRequestOptions {
        public BadPullRequestOptions() {
            super(PullRequestOptions.builder(1));
        }

        @Override
        @NonNull
        public String toJson() {
            StringBuilder sb = JsonUtils.beginJson();
            JsonUtils.addField(sb, BATCH, 1);
            JsonUtils.addFldWhenTrue(sb, NO_WAIT, true);
            JsonUtils.addFieldAsNanos(sb, IDLE_HEARTBEAT, Duration.ofMillis(1));
            return JsonUtils.endJson(sb).toString();
        }
    }

    @Test
    public void testExceedsMaxRequestBytesNthMessageSyncSub() throws Exception {
        Listener listener = new Listener();
        runInSharedOwnNc(listener, (nc, ctx) -> {
            listener.queueStatus(PullWarning, CONFLICT_CODE);
            String dur = random();
            ctx.jsm.addOrUpdateConsumer(ctx.stream, builder().durable(dur).ackPolicy(AckPolicy.None).filterSubjects(ctx.subject()).build());
            PullSubscribeOptions so = PullSubscribeOptions.bind(ctx.stream, dur);
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), so);

            // subject 7 + reply 52 + bytes 100 = 159
            // subject 7 + reply 52 + bytes 100 + headers 21 = 180
            ctx.js.publish(ctx.subject(), new byte[100]);
            ctx.js.publish(ctx.subject(), new Headers().add("foo", "bar"), new byte[100]);
            // 1000 - 159 - 180 = 661
            // subject 7 + reply 52 + bytes 610 = 669 > 661
            ctx.js.publish(ctx.subject(), new byte[610]);

            sub.pull(PullRequestOptions.builder(10).maxBytes(1000).expiresIn(1000).build());
            assertNotNull(sub.nextMessage(500));
            assertNotNull(sub.nextMessage(500));
            assertNull(sub.nextMessage(500));
            listener.validate();
        });
    }

    @Test
    public void testDoesNotExceedMaxRequestBytesExactBytes() throws Exception {
        Listener listener = new Listener();
        runInSharedOwnNc(listener, (nc, ctx) -> {
            listener.queueStatus(PullWarning, CONFLICT_CODE);
            ctx.stream = randomWide(6); // six letters so I can count
            String subject = randomWide(5); // five letters so I can count
            String durable = randomWide(10); // short keeps under max bytes
            ctx.createOrReplaceStream(subject);
            ctx.jsm.addOrUpdateConsumer(ctx.stream, builder().durable(durable).ackPolicy(AckPolicy.None).filterSubjects(subject).build());
            PullSubscribeOptions so = PullSubscribeOptions.bind(ctx.stream, durable);
            JetStreamSubscription sub = ctx.js.subscribe(subject, so);

            // 159 + 180 + 661 = 1000 // subject includes crlf
            // subject 7 + reply 52 + bytes 100 = 159
            // subject 7 + reply 52 + bytes 100 + headers 21 = 180
            // subject 7 + reply 52 + bytes 602 = 661
            ctx.js.publish(subject, new byte[100]);
            ctx.js.publish(subject, new Headers().add("foo", "bar"), new byte[100]);
            ctx.js.publish(subject, new byte[602]);

            sub.pull(PullRequestOptions.builder(10).maxBytes(1000).expiresIn(1000).build());
            assertNotNull(sub.nextMessage(500));
            assertNotNull(sub.nextMessage(500));
            assertNotNull(sub.nextMessage(500));
            assertNull(sub.nextMessage(500)); // there are no more messages
            listener.validateNotReceived();
        });
    }

    @Test
    public void testReader() throws Exception {
        runInShared((nc, ctx) -> {
            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(ctx.consumerName()).filterSubjects(ctx.subject()).build();
            ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);

            PullSubscribeOptions so = PullSubscribeOptions.bind(ctx.stream, ctx.consumerName());
            JetStreamSubscription sub = ctx.js.subscribe(ctx.subject(), so);
            JetStreamReader reader = sub.reader(500, 125);

            int stopCount = 500;

            // create the consumer then use it
            AtomicInteger count = new AtomicInteger();
            Thread readerThread = getReaderThread(count, stopCount, reader);

            Publisher publisher = new Publisher(ctx.js, ctx.subject(), 25);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            readerThread.join();
            publisher.stop();
            pubThread.join();

            assertTrue(count.incrementAndGet() > 500);
        });
    }

    private static Thread getReaderThread(AtomicInteger count, int stopCount, JetStreamReader reader) {
        Thread readerThread = new Thread(() -> {
            try {
                while (count.get() < stopCount) {
                    Message msg = reader.nextMessage(1000);
                    if (msg != null) {
                        msg.ack();
                        count.incrementAndGet();
                    }
                }

                Thread.sleep(50); // allows more messages to come across
                reader.stop();

                Message msg = reader.nextMessage(Duration.ofMillis(1000)); // also coverage next message
                while (msg != null) {
                    msg.ack();
                    count.incrementAndGet();
                    msg = reader.nextMessage(1000);
                }
            }
            catch (Exception e) {
                fail(e);
            }
        });
        readerThread.start();
        return readerThread;
    }

    @Test
    public void testOverflow() throws Exception {
        runInShared(VersionUtils::atLeast2_11, (nc, ctx) -> {
            jsPublish(ctx.js, ctx.subject(), 100);

            // Setting PriorityPolicy requires at least one PriorityGroup to be set
            ConsumerConfiguration ccNoGroup = ConsumerConfiguration.builder()
                .priorityPolicy(PriorityPolicy.Overflow)
                .build();
            JetStreamApiException jsae = assertThrows(JetStreamApiException.class,
                () -> ctx.jsm.addOrUpdateConsumer(ctx.stream, ccNoGroup));
            assertEquals(10159, jsae.getApiErrorCode());

            // Testing errors
            String group = random();
            String consumer = random();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .name(consumer)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .filterSubjects(ctx.subject()).build();
            ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);

            PullSubscribeOptions so = PullSubscribeOptions.fastBind(ctx.stream, consumer);
            JetStreamSubscription sub = ctx.js.subscribe(null, so);

            // 400 Bad Request - Priority Group missing
            sub.pull(1);
            assertThrows(JetStreamStatusException.class, () -> sub.nextMessage(1000));

            // 400 Bad Request - Invalid Priority Group
            sub.pull(PullRequestOptions.builder(5).group("bogus").build());
            assertThrows(JetStreamStatusException.class, () -> sub.nextMessage(1000));

            // Testing min ack pending
            group = random();
            consumer = random();

            cc = ConsumerConfiguration.builder()
                .name(consumer)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .ackWait(60_000)
                .filterSubjects(ctx.subject()).build();
            ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);

            so = PullSubscribeOptions.fastBind(ctx.stream, consumer);
            JetStreamSubscription subPrime = ctx.js.subscribe(null, so);
            JetStreamSubscription subOver = ctx.js.subscribe(null, so);

            PullRequestOptions proNoMin = PullRequestOptions.builder(5)
                .group(group)
                .build();

            PullRequestOptions proOverA = PullRequestOptions.builder(5)
                .group(group)
                .minAckPending(5)
                .build();

            PullRequestOptions proOverB = PullRequestOptions.builder(5)
                .group(group)
                .minAckPending(10)
                .build();

            _overflowCheck(subPrime, proNoMin, true, 5);
            _overflowCheck(subOver, proNoMin, true, 5);

            _overflowCheck(subPrime, proNoMin, false, 5);
            _overflowCheck(subOver, proOverA, true, 5);
            _overflowCheck(subOver, proOverB, true, 0);

            // Testing min pending
            group = random();
            consumer = random();

            cc = ConsumerConfiguration.builder()
                .name(consumer)
                .priorityPolicy(PriorityPolicy.Overflow)
                .priorityGroups(group)
                .filterSubjects(ctx.subject()).build();
            ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);

            so = PullSubscribeOptions.fastBind(ctx.stream, consumer);
            subPrime = ctx.js.subscribe(null, so);
            subOver = ctx.js.subscribe(null, so);

            proNoMin = PullRequestOptions.builder(5)
                .group(group)
                .build();

            proOverA = PullRequestOptions.builder(5)
                .group(group)
                .minPending(78)
                .build();

            _overflowCheck(subPrime, proNoMin, true, 5);
            _overflowCheck(subOver, proNoMin, true, 5);
            _overflowCheck(subOver, proOverA, true, 5);
            _overflowCheck(subOver, proOverA, true, 5);
            // exactly 80 messages now pending, gt or eq to pull min pending for 3 (80, 79, 78)
            _overflowCheck(subOver, proOverA, true, 3);
            // exactly 77 messages now pending lt pull min pending
            _overflowCheck(subOver, proOverA, true, 0);
        });
    }

    private static void _overflowCheck(JetStreamSubscription sub, PullRequestOptions pro, boolean ack, int expected) throws InterruptedException {
        sub.pull(pro);
        int count = 0;
        Message m = sub.nextMessage(1000);
        while (m != null) {
            count++;
            if (ack) {
                m.ack();
            }
            m = sub.nextMessage(100);
        }
        assertEquals(expected, count);
    }

    @Test
    public void testPrioritized() throws Exception {
        // PriorityPolicy.Prioritized
        // start a priority 1 (#1) and a priority 2 (#2) consumer, #1 should get messages, #2 should get none
        // close the #1, #2 should get messages
        // start another priority 1 (#3), #2 should stop getting messages #3 should get messages
        runInShared(VersionUtils::atLeast2_12, (nc, ctx) -> {
            String consumer = random();
            String group = random();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .filterSubject(ctx.subject())
                .name(consumer)
                .priorityGroups(group)
                .priorityPolicy(PriorityPolicy.Prioritized)
                .build();

            StreamContext streamContext = nc.getStreamContext(ctx.stream);
            ConsumerContext consumerContext1 = streamContext.createOrUpdateConsumer(cc);
            ConsumerContext consumerContext2 = streamContext.getConsumerContext(consumer);

            AtomicInteger count1 = new AtomicInteger();
            CountDownLatch latch1 = new CountDownLatch(20);
            MessageHandler handler1 = msg -> {
                msg.ack();
                count1.incrementAndGet();
                latch1.countDown();
            };

            AtomicInteger count2 = new AtomicInteger();
            CountDownLatch latch2 = new CountDownLatch(20);
            MessageHandler handler2 = msg -> {
                msg.ack();
                count2.incrementAndGet();
                latch2.countDown();
            };

            AtomicInteger count3 = new AtomicInteger();
            MessageHandler handler3 = msg -> {
                msg.ack();
                count3.incrementAndGet();
            };

            ConsumeOptions coP1 = ConsumeOptions.builder()
                .batchSize(10)
                .group(group)
                .priority(1)
                .build();
            ConsumeOptions coP2 = ConsumeOptions.builder()
                .batchSize(10)
                .group(group)
                .priority(2)
                .build();

            MessageConsumer mc1 = consumerContext1.consume(coP1, handler1);
            MessageConsumer mc2 = consumerContext2.consume(coP2, handler2);

            AtomicBoolean pub = new AtomicBoolean(true);
            Thread t = new Thread(() -> {
                int count = 0;
                while (pub.get()) {
                    ++count;
                    try {
                        ctx.js.publish(ctx.subject(), ("x" + count).getBytes());
                        sleep(20);
                    }
                    catch (Exception e) {
                        fail(e);
                        return;
                    }
                }
            });
            t.start();

            if (!latch1.await(5, TimeUnit.SECONDS)) {
                fail("Didn't get messages consumer 1");
            }
            assertEquals(0, count2.get());
            mc1.close();

            if (!latch2.await(5, TimeUnit.SECONDS)) {
                fail("Didn't get messages consumer 2");
            }
            MessageConsumer mc3 = consumerContext2.consume(coP1, handler3);

            Thread.sleep(200);
            pub.set(false);
            t.join();
            mc2.close();
            mc3.close();

            assertTrue(count1.get() >= 20);
            assertTrue(count2.get() >= 20);
            assertTrue(count3.get() > 0);
        });
    }

    @Test
    public void testPinnedClient() throws Exception {
        // have 3 consumers in the same group all PriorityPolicy.PinnedClient
        // start consuming, tracking pin ids and counts
        // unpin 10 times and make sure that new pins are made
        runInShared(VersionUtils::atLeast2_12, (nc, ctx) -> {
            String consumer = random();
            String group = random();

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .filterSubject(ctx.subject())
                .name(consumer)
                .priorityGroups(group)
                .priorityPolicy(PriorityPolicy.PinnedClient)
                .build();

            StreamContext streamContext = nc.getStreamContext(ctx.stream);
            ConsumerContext consumerContext1 = streamContext.createOrUpdateConsumer(cc);
            ConsumerContext consumerContext2 = streamContext.getConsumerContext(consumer);
            ConsumerContext consumerContext3 = streamContext.getConsumerContext(consumer);

            //noinspection resource
            assertThrows(IOException.class, () -> consumerContext1.fetchMessages(10));

            Set<String> pinIds = new HashSet<>();
            AtomicInteger count1 = new AtomicInteger();
            AtomicInteger count2 = new AtomicInteger();
            AtomicInteger count3 = new AtomicInteger();
            MessageHandler handler1 = msg -> {
                msg.ack();
                assertNotNull(msg.getHeaders());
                String natsPinId = msg.getHeaders().getFirst(NATS_PIN_ID_HDR);
                assertNotNull(natsPinId);
                pinIds.add(natsPinId);
                count1.incrementAndGet();
            };
            MessageHandler handler2 = msg -> {
                msg.ack();
                assertNotNull(msg.getHeaders());
                String natsPinId = msg.getHeaders().getFirst(NATS_PIN_ID_HDR);
                assertNotNull(natsPinId);
                pinIds.add(natsPinId);
                count2.incrementAndGet();
            };
            MessageHandler handler3 = msg -> {
                msg.ack();
                assertNotNull(msg.getHeaders());
                String natsPinId = msg.getHeaders().getFirst(NATS_PIN_ID_HDR);
                assertNotNull(natsPinId);
                pinIds.add(natsPinId);
                count3.incrementAndGet();
            };

            ConsumeOptions co = ConsumeOptions.builder()
                .batchSize(10)
                .expiresIn(1000)
                .group(group)
                .build();

            MessageConsumer mc1 = consumerContext1.consume(co, handler1);
            MessageConsumer mc2 = consumerContext2.consume(co, handler2);
            MessageConsumer mc3 = consumerContext3.consume(co, handler3);

            AtomicBoolean pub = new  AtomicBoolean(true);
            Thread t = new Thread(() -> {
                int count = 0;
                while (pub.get()) {
                    ++count;
                    try {
                        ctx.js.publish(ctx.subject(), ("x" + count).getBytes());
                        sleep(20);
                    }
                    catch (Exception e) {
                        fail(e);
                        return;
                    }
                }
            });
            t.start();

            int unpins = 0;
            while (unpins++ < 10) {
                sleep(650);
                switch (ThreadLocalRandom.current().nextInt(0, 4)) {
                    case 0:
                        assertTrue(consumerContext1.unpin(group));
                        break;
                    case 1:
                        assertTrue(consumerContext2.unpin(group));
                        break;
                    case 2:
                        assertTrue(consumerContext3.unpin(group));
                        break;
                    case 3:
                        assertTrue(ctx.jsm.unpinConsumer(ctx.stream, consumer, group));
                        break;
                }
                assertTrue(consumerContext1.unpin(group));
            }
            sleep(650);

            pub.set(false);
            t.join();
            mc1.close();
            mc2.close();
            mc3.close();

            assertTrue(pinIds.size() > 3);
            int c1 = count1.get();
            int c2 = count2.get();
            int c3 = count3.get();
            if (c1 > 0) {
                assertTrue(c2 > 0 || c3 > 0);
            }
            else if (c2 > 0) {
                assertTrue(c3 > 0);
            }
            else {
                fail("At least 2 consumers should have gotten messages");
            }
        });
    }
}
