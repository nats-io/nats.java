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

import static io.nats.examples.jetstream.NatsJsUtils.*;

/**
 * This example will demonstrate miscellaneous uses cases of a pull subscription of:
 * batch size and expires pull: <code>pullExpiresIn(int batchSize, Duration expiresIn)</code>,
 * requiring manual handling of null and 408 status.
 */
public class NatsJsPullSubExpireUseCases {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPullSubExpireUseCases [-s server] [-strm stream] [-sub subject] [-dur durable]"
                    + "\n\nDefault Values:"
                    + "\n   [-strm stream]     expire-uc-ack-stream"
                    + "\n   [-sub subject]     expire-uc-ack-subject"
                    + "\n   [-dur durable]     expire-uc-ack-durable"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("expire-uc-ack-stream")
                .defaultSubject("expire-uc-ack-subject")
                .defaultDurable("expire-uc-ack-durable")
                //.uniqueify() // uncomment to be able to re-run without re-starting server
                .build(args, usageString);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            NatsJsUtils.createOrUpdateStream(nc, exArgs.stream, exArgs.subject);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(exArgs.durable)      // required
                    .build();

            // 0.1 Initialize. subscription
            // 0.2 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
            // 0.3 Start the pull, you don't have to call this again because AckMode.NEXT
            // -  When we ack a batch message the server starts preparing or adding to the next batch.
            System.out.println("\n----------\n0. Initialize the subscription and pull.");
            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            sub.pullExpiresIn(10, Duration.ofSeconds(2));

            // 1. Publish some that is less than the batch size.
            System.out.println("\n----------\n1. Publish some amount of messages, but not entire batch size.");
            publish(js, exArgs.subject, "A", 6);
            List<Message> messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 6 total messages, we received: " + messages.size());
            System.out.println("We should have received 6 regular messages, we received: " + countJs(messages));
            System.out.println("We should have received 0 408 markers of last batch, we received: " + count408s(messages));

            // 2. Publish some more covering our pull size...
            System.out.println("----------\n2. Publish more than the batch size.");
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            publish(js, exArgs.subject, "B", 14);
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 14 total messages, we received: " + messages.size());
            System.out.println("We should have received 10 regular messages, we received: " + countJs(messages));
            System.out.println("We should have received 4 408 markers of last batch, we received: " + count408s(messages));

            // 3. There are still 4 messages from B, but the batch was finished
            // -  won't get any messages until a pull is issued.
            System.out.println("----------\n3. Read without re-issue.");
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 0 total messages, we received: " + messages.size());

            // 4. re-issue the pull to get the last 4
            System.out.println("----------\n4. Re-issue to get the last 4.");
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 4 total messages, we received: " + messages.size());
            System.out.println("We should have received 4 regular messages, we received: " + countJs(messages));
            System.out.println("We should have received 0 408 markers of last batch, we received: " + count408s(messages));

            // 5. publish a lot of messages
            System.out.println("----------\n5. Publish a lot of messages. The last pull was under the batch size.");
            publish(js, exArgs.subject, "C", 25);
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 16 total messages, we received: " + messages.size());
            System.out.println("We should have received 10 regular messages, we received: " + countJs(messages));
            System.out.println("We should have received 6 408 markers of last batch, we received: " + count408s(messages));

            // 6. there are still more messages
            System.out.println("----------\n6. Still more messages.");
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 1 total messages, we received: " + messages.size());
            System.out.println("We should have received 10 regular messages, we received: " + countJs(messages));
            System.out.println("We should have received 0 408 markers of last batch, we received: " + count408s(messages));

            // 7. there are still more messages
            System.out.println("----------\n7. Still more messages.");
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 5 total messages, we received: " + messages.size());
            System.out.println("We should have received 5 regular messages, we received: " + countJs(messages));
            System.out.println("We should have received 0 408 markers of last batch, we received: " + count408s(messages));

            // 8. we got them all
            System.out.println("----------\n7. No messages left.");
            sub.pullExpiresIn(10, Duration.ofSeconds(2));
            messages = readMessagesAck(sub, Duration.ofSeconds(2));
            System.out.println("We should have received 5 total messages, we received: " + messages.size());
            System.out.println("We should have received 0 regular messages, we received: " + countJs(messages));
            System.out.println("We should have received 5 408 markers of last batch, we received: " + count408s(messages));

            System.out.println("----------\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
