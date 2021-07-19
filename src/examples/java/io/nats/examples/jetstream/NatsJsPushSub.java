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

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static io.nats.examples.jetstream.NatsJsUtils.streamExists;

/**
 * This example will demonstrate JetStream push subscribing. Run NatsJsPub first to setup message data.
 */
public class NatsJsPushSub {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPushSub [-s server] [-strm stream] [-sub subject] [-mcnt msgCount] [-dur durable] [-dlvr deliver]"
                    + "\n\nDefault Values:"
                    + "\n   [-strm stream]     example-stream"
                    + "\n   [-sub subject]     example-subject"
                    + "\n   [-mcnt msgCount]   0"
                    + "\n\nRun Notes:"
                    + "\n   - durable is optional, durable behaves differently, try it by running this twice with durable set"
                    + "\n   - deliver is optional"
                    + "\n   - msg_count < 1 will just loop until there are no more messages"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("example-stream")
                .defaultSubject("example-subject")
                .defaultMsgCount(0)
//                .defaultDurable("push-sub-durable")
//                .defaultDeliver("push-sub-deliver")
                .build(args, usageString);

        int count = exArgs.msgCount < 1 ? Integer.MAX_VALUE : exArgs.msgCount;

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            if (!streamExists(nc, exArgs.stream)) {
                System.out.println("Stopping program, stream does not exist: " + exArgs.stream);
                return;
            }

            // just some reporting
            System.out.println("\nConnected to server " + exArgs.server + ". Listening to...");
            System.out.println("  Subject: " + exArgs.subject);
            if (exArgs.durable != null) {
                System.out.println("  Durable: " + exArgs.durable);
            }
            if (exArgs.deliver != null) {
                System.out.println("  Deliver Subject: " + exArgs.deliver);
            }
            if (count == Integer.MAX_VALUE) {
                System.out.println("  Until there are no more messages");
            }
            else {
                System.out.println("  For " + count + " messages max");
            }

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // A push subscription means the server will "push" us messages.
            // Build our subscription options. We'll create a durable subscription.
            // Durable means the server will remember where we are if we use that name.
            PushSubscribeOptions so = PushSubscribeOptions.builder()
                    .stream(exArgs.stream) // saves a lookup
                    .durable(exArgs.durable)
                    .deliverSubject(exArgs.deliver)
                    .build();

            // Subscribe synchronously, then just wait for messages.
            JetStreamSubscription sub = js.subscribe(exArgs.subject, so);
            nc.flush(Duration.ofSeconds(5));

            int red = 0;
            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            while (msg != null) {
                System.out.println("\nMessage Received:");
                if (msg.hasHeaders()) {
                    System.out.println("  Headers:");
                    for (String key: msg.getHeaders().keySet()) {
                        for (String value : msg.getHeaders().get(key)) {
                            System.out.printf("    %s: %s\n", key, value);
                        }
                    }
                }

                System.out.printf("  Subject: %s\n  Data: %s\n",
                        msg.getSubject(),
                        new String(msg.getData(), StandardCharsets.UTF_8));

                // This check may not be necessary for this example depending
                // on how the consumer has been setup.  When a deliver subject
                // is set on a consumer, messages can be received from applications
                // that are NATS producers and from streams in NATS servers.
                if (msg.isJetStream()) {
                    System.out.println("  " + msg.metaData());
                    // Because this is a synchronous subscriber, there's no auto-ack.
                    // We need to ack the message or it'll be redelivered.  
                    msg.ack();
                }

                ++red;
                if (--count == 0) {
                    msg = null;
                }
                else {
                    msg = sub.nextMessage(Duration.ofSeconds(1));
                }
            }

            System.out.println("\n" + red + " message(s) were received.\n");

            sub.unsubscribe();
            nc.flush(Duration.ofSeconds(5));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
