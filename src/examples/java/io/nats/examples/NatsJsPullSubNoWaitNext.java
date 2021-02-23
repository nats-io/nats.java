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
import java.util.concurrent.ThreadLocalRandom;

/**
 * This example will demonstrate a pull subscription with:
 * - a batch size  + nowait i.e. subscription.pullNoWait(10)
 * - AckMode.NEXT
 *
 * Usage: java NatsJsPullSubNoWaitNext [-s server] [-strm stream] [-sub subject] [-dur durable]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsPullSubNoWaitNext extends NatsJsPullSubBase {

    public static void main(String[] args) {

        long uniqueEnough = System.currentTimeMillis();

        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("nowait-next-stream" + uniqueEnough)
                .defaultSubject("nowait-next-subject" + uniqueEnough)
                .defaultDurable("nowait-next-durable" + uniqueEnough)
                .build(args);
        
        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {

            createStream(nc, exArgs.stream, exArgs.subject);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(exArgs.durable) // required
                    .ackMode(PullSubscribeOptions.AckMode.NEXT) // NEXT is NOT the default
                    // .configuration(...)   // if you want a custom io.nats.client.ConsumerConfiguration
                    .build();

            // 0.1 Initialize. subscription
            // 0.2 DO NOT start the pull, no wait works differently than regular pull.
            //     With no wait, we have to start the pull the first time and every time the
            //     batch size is exhausted or no waits out.
            // 0.3 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            System.out.println("\n----------");

            int PULL_SIZE = 5;

            int pulls = 0;
            int round = 0;
            int twoCauses = 2;
            while (pulls < 3 && round++ < 10) {
                int publish = ThreadLocalRandom.current().nextInt(PULL_SIZE * 2); // 0 to 2x-1
                System.out.print("" + pad(round) + ". Pub: " + pad(publish) + " msgs | Pull: " + yn(twoCauses > 1) + " |");
                if (publish > 0) {
                    String prefix = "" + (char)((round-1) % 26 + 65);
                    publish(js, exArgs.subject, prefix, publish, false);
                }
                if (twoCauses > 1) {
                    sub.pullNoWait(10);
                    twoCauses = 0;
                    pulls++;
                }
                List<Message> messages = readMessagesAck(sub, false);
                System.out.print(" Red: " + pad(messages.size()) + " msgs |");

                if (messages.size() == 0) {
                    twoCauses++;
                }
                for (Message m : messages) {
                    if (m.isStatusMessage()) {
                        System.out.print(" !" + m.getStatus().getCode());
                        twoCauses++;
                    }
                    else {
                        System.out.print(" " + new String(m.getData()));
                    }
                }

                System.out.println();
            }

            System.out.println("----------\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
