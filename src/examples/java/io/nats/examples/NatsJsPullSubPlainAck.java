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

import java.time.Duration;
import java.util.List;

/**
 * This example will demonstrate a pull subscription with:
 * - a batch size (plain) i.e. subscription.pull(10)
 * - AckMode.ACK
 *
 * Usage: java NatsJsPullSubPlainAck [-s server] [-strm stream] [-sub subject] [-dur durable]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsPullSubPlainAck extends NatsJsPullSubBase {

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("plain-ack-stream")
                .defaultSubject("plain-ack-subject")
                .defaultDurable("plain-ack-durable")
                .build(args);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            NatsJsUtils.createOrUpdateStream(nc, exArgs.stream, exArgs.subject);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(exArgs.durable)      // required
                    .ackMode(PullSubscribeOptions.AckMode.ACK) // ACK is default so not actually required
                    // .configuration(...) // if you want a custom io.nats.client.ConsumerConfiguration
                    .build();

            // 0.1 Initialize. subscription
            // 0.2 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
            // 0.3 Start the pull, you don't have to call this again because AckMode.NEXT
            // -  When we ack a batch message the server starts preparing or adding to the next batch.
            System.out.println("\n----------\n0. Initialize the subscription and pull.");
            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            sub.pull(10);

            // 1. Publish some that is less than the batch size.
            // -  Do this first as data will typically be published first.
            System.out.println("\n----------\n1. Publish some amount of messages, but not entire batch size.");
            publish(js, exArgs.subject, "A", 4);
            List<Message> messages = readMessagesAck(sub);
            System.out.println("We should have received 4 total messages, we received: " + messages.size());

            // 2. Publish some more covering our pull size...
            // -  Read what is available, expect only 6 b/c 4 + 6 = 10
            System.out.println("----------\n2. Publish more than the batch size.");
            publish(js, exArgs.subject, "B", 10);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 6 total messages, we received: " + messages.size());

            // 3. There are still 4 messages from B, but the batch was finished
            // -  won't get any messages until a pull is issued.
            System.out.println("----------\n3. Read without re-issue.");
            messages = readMessagesAck(sub);
            System.out.println("We should have received 0 total messages, we received: " + messages.size());

            // 4. re-issue the pull to get the last 4
            System.out.println("----------\n4. Re-issue to get the last 4.");
            sub.pull(10);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 4 total messages, we received: " + messages.size());

            System.out.println("----------\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
