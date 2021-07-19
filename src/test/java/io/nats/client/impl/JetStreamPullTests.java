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
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPullTests extends JetStreamTestBase {

    @Test
    public void testFetch() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .ackWait(Duration.ofMillis(2500))
                    .build();

            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .durable(DURABLE) // required
                    .configuration(cc)
                    .build();

            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            assertSubscription(sub, STREAM, DURABLE, null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            List<Message> messages = sub.fetch(10, Duration.ofSeconds(3));
            validateRead(0, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, SUBJECT, "A", 10);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, SUBJECT, "B", 20);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            messages = sub.fetch(10, Duration.ofSeconds(3));
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, SUBJECT, "C", 5);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            validateRead(5, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, SUBJECT, "D", 15);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            messages = sub.fetch(10, Duration.ofSeconds(3));
            validateRead(5, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, SUBJECT, "E", 10);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            validateRead(10, messages.size());
            sleep(3000);

            messages = sub.fetch(10, Duration.ofSeconds(3));
            validateRead(10, messages.size());
            messages.forEach(Message::ack);
        });
    }

    @Test
    public void testIterate() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .ackWait(Duration.ofMillis(2500))
                    .build();

            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .durable(DURABLE) // required
                    .configuration(cc)
                    .build();

            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            assertSubscription(sub, STREAM, DURABLE, null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            Iterator<Message> iterator = sub.iterate(10, Duration.ofSeconds(3));
            List<Message> messages = readMessages(iterator);
            validateRead(0, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, SUBJECT, "A", 10);
            iterator = sub.iterate(10, Duration.ofSeconds(3));
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, SUBJECT, "B", 20);
            iterator = sub.iterate(10, Duration.ofSeconds(3));
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            iterator = sub.iterate(10, Duration.ofSeconds(3));
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, SUBJECT, "C", 5);
            iterator = sub.iterate(10, Duration.ofSeconds(3));
            messages = readMessages(iterator);
            validateRead(5, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, SUBJECT, "D", 15);
            iterator = sub.iterate(10, Duration.ofSeconds(3));
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);

            iterator = sub.iterate(10, Duration.ofSeconds(3));
            messages = readMessages(iterator);
            validateRead(5, messages.size());
            messages.forEach(Message::ack);

            jsPublish(js, SUBJECT, "E", 10);
            iterator = sub.iterate(10, Duration.ofSeconds(3));
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            sleep(3000);

            iterator = sub.iterate(10, Duration.ofSeconds(3));
            messages = readMessages(iterator);
            validateRead(10, messages.size());
            messages.forEach(Message::ack);
        });
    }

    @Test
    public void testPlain() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(DURABLE).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            assertSubscription(sub, STREAM, DURABLE, null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish some amount of messages, but not entire pull size
            jsPublish(js, SUBJECT, "A", 4);

            // start the pull
            sub.pull(10);

            // read what is available, expect 4
            List<Message> messages = readMessagesAck(sub);
            int total = messages.size();
            validateRedAndTotal(4, messages.size(), 4, total);

            // publish some more covering our initial pull and more
            jsPublish(js, SUBJECT, "B", 10);

            // read what is available, expect 6 more
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(6, messages.size(), 10, total);

            // read what is available, should be zero since we didn't re-pull
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 10, total);

            // re-issue the pull
            sub.pull(10);

            // read what is available, should be 4 left over
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(4, messages.size(), 14, total);

            // publish some more
            jsPublish(js, SUBJECT, "C", 10);

            // read what is available, should be 6 since we didn't finish the last batch
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(6, messages.size(), 20, total);

            // re-issue the pull, but a smaller amount
            sub.pull(2);

            // read what is available, should be 5 since we changed the pull size
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(2, messages.size(),22, total);

            // re-issue the pull, since we got the full batch size
            sub.pull(2);

            // read what is available, should be zero since we didn't re-pull
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(2, messages.size(), 24, total);

            // re-issue the pull, any amount there are no messages
            sub.pull(1);

            // read what is available, there are none
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 24, total);
        });
    }

    @Test
    public void testNoWait() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(DURABLE).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            assertSubscription(sub, STREAM, DURABLE, null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish 10 messages
            // no wait, batch size 10, there are 10 messages, we will read them all and not trip nowait
            jsPublish(js, SUBJECT, "A", 10);
            sub.pullNoWait(10);
            List<Message> messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            // publish 20 messages
            // no wait, batch size 10, there are 20 messages, we will read 10
            jsPublish(js, SUBJECT, "B", 20);
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
            jsPublish(js, SUBJECT, "C", 5);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(6, messages.size());
            assertLastIsStatus(messages, 404);

            // publish 12 messages
            // no wait, batch size 10, there are more than batch messages we will read 10
            jsPublish(js, SUBJECT, "D", 12);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(10, messages.size());

            // 2 messages left
            // no wait, less than batch ssize will WILL trip nowait
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(3, messages.size());
            assertLastIsStatus(messages, 404);
        });
    }

    @Test
    public void testAfterIncompleteExpiresPulls() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(DURABLE).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            assertSubscription(sub, STREAM, DURABLE, null, true);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            long expires = 500; // millis

            // publish 10 messages
            jsPublish(js, SUBJECT, "A", 5);
            sub.pullExpiresIn(10, Duration.ofMillis(expires)); // using Duration version here
            List<Message> messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, SUBJECT, "B", 10);
            sub.pullExpiresIn(10, Duration.ofMillis(expires)); // using Duration version here
            messages = readMessagesAck(sub);
            assertEquals(15, messages.size());
            assertStarts408(messages, 5, 10);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, SUBJECT, "C", 5);
            sub.pullExpiresIn(10, Duration.ofMillis(expires)); // using Duration version here
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, SUBJECT, "D", 10);
            sub.pull(10);
            messages = readMessagesAck(sub);
            assertEquals(15, messages.size());
            assertStarts408(messages, 5, 10);

            jsPublish(js, SUBJECT, "E", 5);
            sub.pullExpiresIn(10, expires);
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, SUBJECT, "F", 10);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(15, messages.size());
            assertStarts408(messages, 5, 10);

            jsPublish(js, SUBJECT, "G", 5);
            sub.pullExpiresIn(10, expires);
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, SUBJECT, "H", 10);
            messages = sub.fetch(10, expires);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            jsPublish(js, SUBJECT, "I", 5);
            sub.pullExpiresIn(10, expires);
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(expires); // make sure the pull actually expires

            jsPublish(js, SUBJECT, "J", 10);
            Iterator<Message> i = sub.iterate(10, expires);
            int count = 0;
            while (i.hasNext()) {
                assertIsJetStream(i.next());
                ++count;
            }
            assertEquals(10, count);
        });
    }

    @Test
    public void testAckNak() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(DURABLE).build();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // NAK
            jsPublish(js, SUBJECT, "NAK", 1);

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
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(DURABLE).build();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // TERM
            jsPublish(js, SUBJECT, "TERM", 1);

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
    public void testAckWaitTimeout() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackWait(Duration.ofMillis(1500)).build();
            PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(DURABLE).configuration(cc).build();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // Ack Wait timeout
            jsPublish(js, SUBJECT, "WAIT", 1);

            sub.pull(1);
            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("WAIT1", data);
            sleep(2000);

            sub.pull(1);
            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("WAIT1", data);

            sub.pull(1);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));
        });
    }

    // @Test
    public void testInProgress() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(DURABLE).build();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);

            // In Progress
            jsPublish(js, SUBJECT, "PRO", 1);

            sub.pull(1);
            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("PRO1", data);
            message.inProgress();
            sleep(500);
//            message.inProgress();
//            sleep(500);
//            message.inProgress();
//            sleep(500);
//            message.inProgress();
//            sleep(500);
            message.ack();

            jsPublish(js, SUBJECT, "PRO", 2);

            sub.pull(1);
            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("PRO2", data);

            sub.pull(1);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));
        });
    }

//    @Test2
    public void testAckSync() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(DURABLE).build();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // ACK Sync
            jsPublish(js, SUBJECT, "ACKSYNC", 1);

            sub.pull(1);
            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("ACKSYNC1", data);
            message.ackSync(Duration.ofSeconds(1));

            sub.pull(1);
            assertNull(sub.nextMessage(Duration.ofSeconds(1)));
        });
    }
}
