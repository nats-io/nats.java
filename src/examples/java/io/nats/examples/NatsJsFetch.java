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
 * - a batch size  + fetch i.e. subscription.fetch(10, Duration.ofSeconds(10))
 *
 * Usage: java NatsJsFetch [-s server] [-strm stream] [-sub subject] [-dur durable]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsFetch extends NatsJsPullSubBase {

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("fetch-ack-stream")
                .defaultSubject("fetch-ack-subject")
                .defaultDurable("fetch-ack-durable")
                .build(args);
        
        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {

            createStream(nc, exArgs.stream, exArgs.subject);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(exArgs.durable) // required
                    // .configuration(...)   // if you want a custom io.nats.client.ConsumerConfiguration
                    .build();

            // 0.1 Initialize. subscription
            // 0.2 DO NOT start the pull, no wait works differently than regular pull.
            //     With no wait, we have to start the pull the first time and every time the
            //     batch size is exhausted or no waits out.
            // 0.3 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
            System.out.println("\n----------\n0. Initialize the subscription and pull.");
            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            // 1. Start the pull, but there are no messages yet.
            // -  Read the messages
            // -  Since there are exactly the batch size we get them all
            //    and do NOT get a nowait status message
            System.out.println("----------\n1. There are no messages yet");
            List<Message> messages = sub.fetch(10, Duration.ofSeconds(3));
            System.out.println("We should have received 0 total messages, we received: " + messages.size());

            // 2. Publish 10 messages
            // -  Start the pull
            // -  Read the messages
            // -  Since there are exactly the batch size we get them all
            //    and do NOT get a nowait status message
            System.out.println("----------\n2. Publish 10 which satisfies the batch");
            publish(js, exArgs.subject, "A", 10);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            messages = readMessagesAck(sub);

            // 3. Publish 20 messages
            // -  Start the pull
            // -  Read the messages
            // -  we do NOT get a nowait status message if there are more or equals messages than the batch
            System.out.println("----------\n3. Publish 20 which is larger than the batch size.");
            publish(js, exArgs.subject, "B", 20);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 4. There are still messages left from the last
            // -  Start the pull
            // -  Read the messages
            // -  we do NOT get a nowait status message if there are more or equals messages than the batch
            System.out.println("----------\n4. Get the rest of the publish.");
            messages = sub.fetch(10, Duration.ofSeconds(3));
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 5. Publish 5 messages
            // -  Start the pull
            // -  Read the messages
            // -  Since there are less than batch size the last message we get will be a status 404 message.
            System.out.println("----------\n5. Publish 5 which is less than batch size.");
            publish(js, exArgs.subject, "C", 5);
            sub.pullNoWait(10);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            System.out.println("We should have received 5 total messages, we received: " + messages.size());

            // 6. Publish 15 messages
            // -  Start the pull
            // -  Read the messages
            // -  we do NOT get a nowait status message if there are more or equals messages than the batch
            System.out.println("----------\n6. Publish 14 which is more than the batch size.");
            publish(js, exArgs.subject, "D", 14);
            messages = sub.fetch(10, Duration.ofSeconds(3));
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 7. There are 5 messages left
            // -  Start the pull
            // -  Read the messages
            // -  Since there are less than batch size the last message we get will be a status 404 message.
            System.out.println("----------\n7. There are 4 messages left, which is less than the batch size.");
            messages = sub.fetch(10, Duration.ofSeconds(3));
            System.out.println("We should have received 5 messages, we received: " + messages.size());

            System.out.println("----------\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
