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

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static io.nats.examples.NatsJsManagement.streamExists;

public class NatsJsSub {

    static final String usageString = "\nUsage: java NatsJsSub [-s server] [-durable durable] [-msgCount #] <streamName> <subject>\n"
            + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        // circumvent the need for command line arguments by uncommenting / customizing the next line
        // args = "hello-stream hello-subject".split(" ");
        // args = "hello-stream hello-subject 2".split(" ");
        // args = "-durable jsSub-durable hello-stream hello-subject".split(" ");
        // args = "-durable jsSub-durable hello-stream hello-subject 2".split(" ");

        ExampleArgs exArgs = ExampleUtils.readJsSubscribeArgs(args, usageString);

        System.out.printf("\nTrying to connect to %s, and listen to %s for %d messages.\n\n", exArgs.server,
                exArgs.subject, exArgs.msgCount);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            if (!streamExists(nc, exArgs.stream)) {
                System.out.println("Stopping program, stream does not exist: " + exArgs.stream);
                return;
            }

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // A push subscription means the server will "push" us messages.
            // Build our subscription options. We'll create a durable subscription.
            // Durable means the server will remember where we are if we use that name.
            PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder();
            if (exArgs.durable != null) {
                builder.durable(exArgs.durable);
            }
            PushSubscribeOptions so = builder.build();

            // Subscribe synchronously, then just wait for messages.
            JetStreamSubscription sub = js.subscribe(exArgs.subject, so);
            nc.flush(Duration.ofSeconds(5));

            boolean noMoreMessages = false;

            int count = exArgs.msgCount < 1 ? Integer.MAX_VALUE : exArgs.msgCount;
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
                    MessageMetaData meta = msg.metaData();
                    System.out.printf("  Stream Name: %s\n", meta.getStream());
                    System.out.printf("  Stream Seq:  %d\n",  meta.streamSequence());
                    System.out.printf("  Consumer Name: %s\n", meta.getConsumer());
                    System.out.printf("  Consumer Seq:  %d\n", meta.consumerSequence());
                    
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
                    noMoreMessages = msg == null;
                }
            }

            System.out.println("\n" + red + " message(s) were received.");

            if (noMoreMessages) {
                System.out.println("There are no more messages on the server.\n");
            }
            else {
                System.out.println("There might be more messages on the server.\n");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}