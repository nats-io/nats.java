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

import static io.nats.examples.jetstream.NatsJsUtils.exitIfStreamNotExists;

/**
 * This example will demonstrate JetStream basic synchronous push subscribing.
 * Run NatsJsPub first to setup message data.
 */
public class NatsJsPushSubBasicSync {

    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsPushSubBasicSync [-s server] [-strm stream] [-sub subject] [-mcnt msgCount] [-dur durable]"
            + "\n\nDefault Values:"
            + "\n   [-strm] example-stream"
            + "\n   [-sub]  example-subject"
            + "\n   [-mcnt] 0"
            + "\n\nRun Notes:"
            + "\n   - make sure you have created and published to the stream and subject, maybe using the NatsJsPub example"
            + "\n   - durable is optional, durable behaves differently, try it by running this twice with durable set"
            + "\n   - msg_count < 1 will just loop until there are no more messages"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Push Subscribe Basic Synchronous", args, usageString)
            .defaultStream("example-stream")
            .defaultSubject("example-subject")
            .defaultMsgCount(0, true) // true indicated 0 means unlimited
            // .defaultDurable("push-sub-basic-sync-durable")
            .build();

        int count = exArgs.msgCount < 1 ? Integer.MAX_VALUE : exArgs.msgCount;

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            // The stream (and data) must exist
            exitIfStreamNotExists(nc, exArgs.stream);

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // Build our subscription options.
            // * A push subscription means the server will "push" us messages.
            // * Durable means the server will remember where we are if we use that name.
            // * Durable can be null or empty, the builder treats them the same.
            // * The stream name is not technically required. If it is not provided, the
            //   code building the subscription will look it up by making a request to the server.
            //   If you know the stream name, you might as well supply it and save a trip to the server.
            PushSubscribeOptions so = PushSubscribeOptions.builder()
                    .stream(exArgs.stream)
                    .durable(exArgs.durable)
                    .build();

            // Subscribe synchronously, then just wait for messages.
            JetStreamSubscription sub = js.subscribe(exArgs.subject, so);
            nc.flush(Duration.ofSeconds(5));

            int red = 0;
            Message msg = sub.nextMessage(Duration.ofSeconds(1)); // timeout can be Duration or millis
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
                    msg.getSubject(), new String(msg.getData(), StandardCharsets.UTF_8));
                System.out.println("  " + msg.metaData());

                // Because this is a synchronous subscriber, there's no auto-ack.
                // The default Consumer Configuration AckPolicy is Explicit
                // so we need to ack the message or it'll be redelivered.
                msg.ack();

                ++red;
                if (--count == 0) {
                    msg = null;
                }
                else {
                    msg = sub.nextMessage(1000); // timeout can be Duration or millis
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
