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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.time.Duration;
import java.util.List;

import static io.nats.examples.jetstream.NatsJsUtils.createStreamThrowWhenExists;
import static io.nats.examples.jetstream.NatsJsUtils.publishDontWait;

/**
 * This example will demonstrate basic use of a pull subscription of:
 * fetch pull: <code>fetch(int batchSize, Duration maxWait)</code>,
 */
public class NatsJsPullSubFetch {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPullSubFetch [-s server] [-strm stream] [-sub subject] [-dur durable]"
                    + "\n\nDefault Values:"
                    + "\n   [-strm stream]     expire-stream"
                    + "\n   [-sub subject]     expire-subject"
                    + "\n   [-dur durable]     expire-durable"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("fetch-stream")
                .defaultSubject("fetch-subject")
                .defaultDurable("fetch-durable")
                .defaultMsgCount(99)
                //.uniqueify() // uncomment to be able to re-run without re-starting server
                .build(args, usageString);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            createStreamThrowWhenExists(nc, exArgs.stream, exArgs.subject);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // start publishing the messages, don't wait for them to finish, simulating an outside producer
            publishDontWait(js, exArgs.subject, "fetch-message", exArgs.msgCount);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(exArgs.durable)      // required
                    // .configuration(...) // if you want a custom io.nats.client.api.ConsumerConfiguration
                    .build();

            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            int red = 0;
            while (red < exArgs.msgCount) {
                List<Message> message = sub.fetch(10, Duration.ofSeconds(1));
                for (Message m : message) {
                    // process message
                    red++;
                    System.out.println("" + red + ". " + m);
                    m.ack();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
