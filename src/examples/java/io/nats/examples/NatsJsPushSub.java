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

import static io.nats.examples.NatsJsUtils.streamExists;

/**
 * This example will demonstrate JetStream push subscribing. Run NatsJsPub first to setup message data.
 *
 * Usage: java NatsJsPushSub [-s server]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsPushSub {

    // STREAM and SUBJECT are required.
    // DURABLE is optional (null), durable behaves differently, try it by running this twice with durable set
    // DELIVER is optional (null)
    // MSG_COUNT < 1 will just loop until there are no more messages
    static final String STREAM = "example-stream";
    static final String SUBJECT = "example-subject";
    static final String DURABLE = null; // "push-sub-durable";
    static final String DELIVER = null; // "push-sub-deliver";
    static final int MSG_COUNT = 0;

    public static void main(String[] args) {
        String server = ExampleArgs.getServer(args);
        int count = MSG_COUNT < 1 ? Integer.MAX_VALUE : MSG_COUNT;

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(args, true))) {

            if (!streamExists(nc, STREAM)) {
                System.out.println("Stopping program, stream does not exist: " + STREAM);
                return;
            }

            // just some reporting
            System.out.println("\nConnected to server " + server + ". Listening to...");
            System.out.println("  Subject: " + SUBJECT);
            if (DURABLE != null) {
                System.out.println("  Durable: " + DURABLE);
            }
            if (DELIVER != null) {
                System.out.println("  Deliver Subject: " + DELIVER);
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
            PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder();
            if (DURABLE != null) {
                builder.durable(DURABLE);
            }
            if (DELIVER != null) {
                builder.deliverSubject(DELIVER);
            }
            PushSubscribeOptions so = builder.build();

            // Subscribe synchronously, then just wait for messages.
            JetStreamSubscription sub = js.subscribe(SUBJECT, so);
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
