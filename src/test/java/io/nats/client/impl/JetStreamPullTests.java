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
    public void plain_AckModeNext_ExpireModeNA() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .durable(DURABLE).ackMode(PullSubscribeOptions.AckMode.NEXT).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish some amount of messages, but not entire pull size
            publish(js, SUBJECT, "A", 4);

            // start the pull
            sub.pull(10);

            // read what is available, expect 4
            List<Message> messages = readMessagesAck(sub);
            int total = messages.size();
            validateRedAndTotal(4, messages.size(), 4, total);

            // publish some more covering our initial pull and more
            publish(js, SUBJECT, "B", 20);

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
    public void plain_AckModeAck_ExpireModeNA() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .durable(DURABLE).ackMode(PullSubscribeOptions.AckMode.ACK).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish some amount of messages, but not entire pull size
            publish(js, SUBJECT, "A", 4);

            // start the pull
            sub.pull(10);

            // read what is available, expect 4
            List<Message> messages = readMessagesAck(sub);
            int total = messages.size();
            validateRedAndTotal(4, messages.size(), 4, total);

            // publish some more covering our initial pull and more
            publish(js, SUBJECT, "B", 10);

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
            publish(js, SUBJECT, "C", 10);

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
    public void noWait_AckModeNext_ExpireModeNA() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .durable(DURABLE)
                    .ackMode(PullSubscribeOptions.AckMode.NEXT)
                    .build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish 10 messages
            // no wait, batch size 10, there are 10 messages, we will read them all and not trip nowait
            publish(js, SUBJECT, "A", 10);
            sub.pullNoWait(10);
            List<Message> messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            // publish 20 messages exact
            // no wait, batch size 10, there are 20 messages, we will read them all
            // but since we are an exact multiple of batch size we do not get a 404
            publish(js, SUBJECT, "B", 20);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(20, messages.size());

            // publish 5 messages
            // no wait, batch size 10, there are 5 messages, we WILL trip nowait
            publish(js, SUBJECT, "C", 5);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(6, messages.size());
            assertLastIsStatus(messages, 404);

            // publish 12 messages
            // no wait, batch size 10, there are more but not exact multiple of batch, we WILL trip nowait
            publish(js, SUBJECT, "D", 12);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(13, messages.size());
            assertLastIsStatus(messages, 404);

            // publish 0 messages
            // no wait, we WILL trip nowait
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(1, messages.size());
            assertLastIsStatus(messages, 404);
        });
    }

    @Test
    public void noWait_AckModeAck_ExpireModeNA() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .durable(DURABLE)
                    .ackMode(PullSubscribeOptions.AckMode.ACK)
                    .build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish 10 messages
            // no wait, batch size 10, there are 10 messages, we will read them all and not trip nowait
            publish(js, SUBJECT, "A", 10);
            sub.pullNoWait(10);
            List<Message> messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            // publish 20 messages
            // no wait, batch size 10, there are 20 messages, we will read 10
            publish(js, SUBJECT, "B", 20);
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
            publish(js, SUBJECT, "C", 5);
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            assertEquals(6, messages.size());
            assertLastIsStatus(messages, 404);

            // publish 12 messages
            // no wait, batch size 10, there are more than batch messages we will read 10
            publish(js, SUBJECT, "D", 12);
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
    public void expireFuture_AckModeNext_ExpireModeLeave() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .durable(DURABLE).ackMode(PullSubscribeOptions.AckMode.NEXT).build();

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

//    @Test
    public void expireImmediately_AckModeNext_ExpireModeLeave() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .expireMode(PullSubscribeOptions.ExpireMode.LEAVE)
                    .durable(DURABLE).ackMode(PullSubscribeOptions.AckMode.NEXT).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish 10 messages
            // expire, batch size 10, there are 10 messages, we will read them all
            publish(js, SUBJECT, 101, 10);
            sub.pullExpiresIn(10, Duration.ZERO);
            List<Message> messages = readMessagesAck(sub);
            assertEquals(10, messages.size());
            assertAllJetStream(messages);

            // publish 15 messages
            publish(js, SUBJECT, 201, 15);
            // expire, batch size 10, there are 15 messages, we will read them all
            // and for expire, since we got more than batch size we do not trip expire
            sub.pullExpiresIn(10, Duration.ZERO);
            messages = readMessagesAck(sub);
            assertEquals(15, messages.size());
            assertAllJetStream(messages);

            // publish 5 messages
            publish(js, SUBJECT, 301, 5);
            // expire, batch size 10, there are 5 messages
            sub.pullExpiresIn(10, Duration.ZERO);
            messages = readMessagesAck(sub);
            assertEquals(1, messages.size());
            assertLastIsStatus(messages, 408);
        });
    }
}
