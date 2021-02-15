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

package io.nats.examples;

import io.nats.client.*;
import io.nats.client.impl.JetStreamApiException;
import io.nats.client.impl.NatsMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * This example will demonstrate a pull subscription with expire in the future.
 *
 * Usage: java NatsJsPullSubWithExpireImmediately [server]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsPullSubWithExpireImmediately {

    // STREAM, SUBJECT, DURABLE are required.
    static final String STREAM = "expire-stream";
    static final String SUBJECT = "expire-subject";
    static final String DURABLE = "expire-durable";

    public static void main(String[] args) {
        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(args))) {
            NatsJsUtils.createOrUpdateStream(nc, STREAM, SUBJECT);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(DURABLE)      // required
                    // .configuration(...) // if you want a custom io.nats.client.ConsumerConfiguration
                    .build();

            // 0.1 Initialize. subscription
            // 0.2 DO NOT start the pull, no wait works differently than regular pull.
            //     With no wait, we have to start the pull the first time and every time the
            //     batch size is exhausted or no waits out.
            // 0.3 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
            System.out.println("\n----------\n0. Initialize the subscription and pull.");
            JetStreamSubscription sub = js.subscribe(SUBJECT, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            // 1. Publish 10 messages
            // -  Start the pull
            // -  Read the messages
            // -  Exactly the batch size, we get them all
            System.out.println("----------\n1. Publish 10 which satisfies the batch");
            publish(js, SUBJECT, "A", 10);
            sub.pullExpiresIn(10, Duration.ZERO);
            List<Message> messages = readMessagesAck(sub);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 2. Publish 15 messages
            // -  Start the pull
            // -  Read the messages
            // -  More than the batch size, we get them all
            System.out.println("----------\n2. Publish 15 which an exact multiple of the batch size.");
            publish(js, SUBJECT, "B", 15);
            sub.pullExpiresIn(10, Duration.ZERO);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 15 total messages, we received: " + messages.size());

            // 3. Publish 5 messages
            // -  Start the pull
            // -  Read the messages
            // -  Less than the batch size, we get them all
            System.out.println("----------\n3. Publish 5 which is less than batch size.");
            publish(js, SUBJECT, "C", 5);
            sub.pullExpiresIn(10, Duration.ZERO);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 5 total messages, we received: " + messages.size());

            // 4. There are no waiting messages.
            // -  Start the pull
            // -  Read the messages
            // -  Since there are no messages the only message will be a status 408 message.
            System.out.println("----------\n4. There are no waiting messages.");
            sub.pullNoWait(10);
            messages = readMessagesAck(sub);
            Message lastMessage = messages.get(0);
            System.out.println("We should have received 1 messages, we received: " + messages.size());
            System.out.println("Should be a status message? " + lastMessage.isStatusMessage() + " " + lastMessage.getStatus());

            System.out.println("----------\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void publish(JetStream js, String subject, String prefix, int count) throws IOException, JetStreamApiException {
        System.out.print("Publish ->");
        for (int x = 1; x <= count; x++) {
            String data = "#" + prefix + "." + x;
            System.out.print(" " + data);
            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data(data.getBytes(StandardCharsets.US_ASCII))
                    .build();
            js.publish(msg);
        }
        System.out.println(" <-");
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub) throws InterruptedException {
        List<Message> messages = new ArrayList<>();
        Message msg = sub.nextMessage(Duration.ofSeconds(1));
        boolean first = true;
        while (msg != null) {
            if (first) {
                first = false;
                System.out.print("Read/Ack ->");
            }
            messages.add(msg);
            if (msg.isJetStream()) {
                msg.ack();
                System.out.print(" " + new String(msg.getData()));
                msg = sub.nextMessage(Duration.ofSeconds(1));
            }
            else {
                msg = null; // so we break the loop
                System.out.print(" !Status! ");
            }
        }
        if (first) {
            System.out.println("No messages available.");
        }
        else {
            System.out.println(" <- ");
        }
        return messages;
    }
}