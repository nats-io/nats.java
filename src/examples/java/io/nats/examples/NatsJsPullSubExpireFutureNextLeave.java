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

import static io.nats.examples.ExampleUtils.sleep;

/**
 * This example will demonstrate a pull subscription with expire in the future.
 *
 * Usage: java NatsJsPullSubExpireFutureNextLeave [-s server] [-strm stream] [-sub subject] [-dur durable]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsPullSubExpireFutureNextLeave extends NatsJsPullSubBase {

    /*
        THIS EXAMPLE IS DRAFT - DO NOT USE
     */
    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("expire-future-next-leave-stream")
                .defaultSubject("expire-future-next-leave-subject")
                .defaultDurable("expire-future-next-leave-durable")
                .build(args);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            NatsJsUtils.createOrUpdateStream(nc, exArgs.stream, exArgs.subject);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(exArgs.durable)      // required
                    .ackMode(PullSubscribeOptions.AckMode.NEXT) // NEXT is NOT the default
                    .expireMode(PullSubscribeOptions.ExpireMode.LEAVE) // LEAVE is NOT the default
                    // .configuration(...) // if you want a custom io.nats.client.ConsumerConfiguration
                    .build();

            // 0.1 Initialize. subscription
            // 0.2 DO NOT start the pull, no wait works differently than regular pull.
            //     With no wait, we have to start the pull the first time and every time the
            //     batch size is exhausted or no waits out.
            // 0.3 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
            System.out.println("\n----------\n0. Initialize the subscription and pull.");
            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            // 1. Publish 10 messages
            // -  Start the pull
            // -  Read the messages
            // -  Exactly the batch size, we get them all
            System.out.println("----------\n1. Publish 10 which satisfies the batch");
            publish(js, exArgs.subject, "A", 10);
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            List<Message> messages = readMessagesAck(sub);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            // 2. Publish 15 messages
            // -  Start the pull
            // -  Read the messages
            // -  More than the batch size, we get them all
            System.out.println("----------\n2. Publish 15 which an exact multiple of the batch size.");
            publish(js, exArgs.subject, "B", 15);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 15 total messages, we received: " + messages.size());

            // 3. Publish 5 messages
            // -  Start the pull
            // -  Read the messages
            // -  Less than the batch size, we get them all
            System.out.println("----------\n3. Publish 5 which is less than batch size.");
            publish(js, exArgs.subject, "C", 5);
            sleep(3000);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 5 total messages, we received: " + messages.size());

            // 4. There are no waiting messages.
            // -  Start the pull
            // -  Read the messages
            // -  Since there are no messages we get nothing.
            System.out.println("----------\n4. There are no waiting messages.");
            sleep(3000);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 0 total messages, we received: " + messages.size());

            // 5. Publish 10 messages, but not in time
            // -  Start the pull
            // -  Read the messages
            // -  Exactly the batch size, we get them all
            System.out.println("----------\n5. Publish 10 which satisfies the batch");
            publish(js, exArgs.subject, "D", 5);
            sleep(3000);
            messages = readMessagesAck(sub);
            System.out.println("We should have received 10 total messages, we received: " + messages.size());

            System.out.println("----------\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
