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
import java.util.List;

public class JetStreamPushTests extends JetStreamTestBase {

    @Test
    public void testPushEphemeral() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // publish some messages
            publish(js, SUBJECT, 1, 5);

            // Build our subscription options.
            PushSubscribeOptions options = PushSubscribeOptions.builder().build();

            // Subscription 1
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available
            List<Message> messages1 = readMessagesAck(sub);
            int total = messages1.size();
            validateRedAndTotal(5, messages1.size(), 5, total);

            // read again, nothing should be there
            List<Message> messages0 = readMessagesAck(sub);
            total += messages0.size();
            validateRedAndTotal(0, messages0.size(), 5, total);

            // Subscription 2
            sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available, same messages
            List<Message> messages2 = readMessagesAck(sub);
            total = messages2.size();
            validateRedAndTotal(5, messages2.size(), 5, total);

            // read again, nothing should be there
            messages0 = readMessagesAck(sub);
            total += messages0.size();
            validateRedAndTotal(0, messages0.size(), 5, total);

            assertSameMessages(messages1, messages2);
        });
    }

    @Test
    public void testPushDurable() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // publish some messages
            publish(js, SUBJECT, 1, 5);

            // use ackWait so I don't have to wait forever before re-subscribing
            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackWait(Duration.ofSeconds(3)).build();

            // Build our subscription options.
            PushSubscribeOptions options = PushSubscribeOptions.builder()
                    .durable(DURABLE)
                    .configuration(cc)
                    .build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available
            List<Message> messages = readMessagesAck(sub);
            int total = messages.size();
            validateRedAndTotal(5, messages.size(), 5, total);

            // read again, nothing should be there
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 5, total);

            sub.unsubscribe(); // don't know if this does anmything
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // re-subscribe
            sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read again, nothing should be there
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 5, total);
        });
    }
}
