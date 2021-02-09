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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JetStreamPullTests extends JetStreamTestBase {

    @Test
    public void testPull() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(DURABLE).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish some amount of messages, but not entire pull size
            publish(js, SUBJECT, 1, 4);

            // start the pull
            sub.pull(10);

            // read what is available, expect 4
            List<Message> messages = readMessagesAck(sub);
            int total = messages.size();
            validateRedAndTotal(4, messages.size(), 4, total);

            // publish some more covering our initial pull and more
            publish(js, SUBJECT, 5, 20);

            // read what is available, expect 20 more
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(20, messages.size(), 24, total);

            // read what is available
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 24, total);
        });
    }

    @Test
    public void testPullNoWait() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(DURABLE).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish 10 messages
            // no wait, batch size 10, there are 10 messages, we will read them all and not trip nowait
            publish(js, SUBJECT, 101, 10);
            sub.pullNoWait(10);
            List<Message> messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            // publish 15 messages
            // no wait, batch size 10, there are 15 messages, we will read them all
            // but since we are in the second block of 10 and there are less than 10, we DO trip the nowait
            publish(js, SUBJECT, 201, 15);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(16, messages.size());
            assertLastIsStatus(messages, 404);

            // publish 5 messages
            // no wait, batch size 10, there are 5 messages, we WILL trip nowait
            publish(js, SUBJECT, 301, 5);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(6, messages.size());
            assertLastIsStatus(messages, 404);
        });
    }

    @Test
    public void testPullExpireFuture() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(DURABLE).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish 10 messages
            // expire, batch size 10, there are 10 messages, we will read them all
            publish(js, SUBJECT, 101, 10);
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            List<Message> messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);
            sleep(2); // make sure we are passed the expiration

            // publish 15 messages
            publish(js, SUBJECT, 201, 15);
            // expire, batch size 10, there are 15 messages, we will read them all
            // and for expire, since we got more than batch size we do not trip expire
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            messages = readMessagesAck(sub);
            assertEquals(15, messages.size());
            assertAllJetStream(messages);
            sleep(2); // make sure we are passed the expiration

            // publish 5 messages
            publish(js, SUBJECT, 301, 5);
            // expire, batch size 10, there are 5 messages
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            messages = readMessagesAck(sub);
            assertEquals(5, messages.size());
            assertAllJetStream(messages);
            sleep(2); // make sure we are passed the expiration

            // expire, batch size 10, there are 0 messages
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            messages = readMessagesAck(sub);
            assertEquals(0, messages.size());
        });
    }

    @Test
    public void testPullExpireImmediately() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder().durable(DURABLE).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish 10 messages
            // expire, batch size 10, there are 10 messages, we will read them all
            publish(js, SUBJECT, 100, 10);
            sub.pullExpiresIn(10, Duration.ZERO);
            List<Message> messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            // publish 15 messages
            publish(js, SUBJECT, 200, 15);
            // expire, batch size 10, there are 15 messages, we will read them all
            // and for expire, since we got more than batch size we do not trip expire
            sub.pullExpiresIn(10, Duration.ZERO);
            messages = readMessagesAck(sub);
            assertEquals(15, messages.size());
            assertAllJetStream(messages);

            // publish 5 messages
            publish(js, SUBJECT, 300, 5);
            // expire, batch size 10, there are 5 messages
            sub.pullExpiresIn(10, Duration.ZERO);
            messages = readMessagesAck(sub);
            assertEquals(1, messages.size());
            assertLastIsStatus(messages, 408);
        });
    }
}
