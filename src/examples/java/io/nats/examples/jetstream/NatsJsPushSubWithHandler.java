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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.examples.jetstream.NatsJsUtils.streamExists;

/**
 * This example will demonstrate JetStream push subscribing with a handler. Run NatsJsPub first to setup message data.
 */
public class NatsJsPushSubWithHandler {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPushSubWithHandler [-s server] [-strm stream] [-sub subject] [-mcnt msgCount] [-dur durable]"
                    + "\n\nDefault Values:"
                    + "\n   [-strm stream]    example-stream"
                    + "\n   [-sub subject]    example-subject"
                    + "\n   [-mcnt msgCount]  "
                    + "\n\nRun Notes:"
                    + "\n   - durable is optional, durable behaves differently, try it by running this twice with durable set"
                    + "\n   - msgCount should be > 0"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("example-stream")
                .defaultSubject("example-subject")
                .defaultMsgCount(3)
//                .defaultDurable("push-sub-handler-durable")
                .build(args, usageString);

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
            System.out.println("  For " + exArgs.msgCount + " messages max");

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            CountDownLatch msgLatch = new CountDownLatch(exArgs.msgCount);
            AtomicInteger received = new AtomicInteger();
            AtomicInteger ignored = new AtomicInteger();

            // create our message handler.
            MessageHandler handler = msg -> {
                if (msgLatch.getCount() == 0) {
                    ignored.incrementAndGet();
                    if (msg.isJetStream()) {
                        System.out.println("Message Ignored, latch count already reached "
                                + new String(msg.getData(), StandardCharsets.UTF_8));
                        msg.nak();
                    }
                }
                else {
                    received.incrementAndGet();
                    System.out.println("\nMessage Received:");

                    if (msg.hasHeaders()) {
                        System.out.println("  Headers:");
                        for (String key : msg.getHeaders().keySet()) {
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
                        msg.ack();
                    }

                    msgLatch.countDown();
                }
            };

            // A push subscription means the server will "push" us messages.
            // Build our subscription options. We'll create a durable subscription.
            // Durable means the server will remember where we are if we use that name.
            PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder();
            if (exArgs.durable != null) {
                builder.durable(exArgs.durable);
            }
            PushSubscribeOptions so = builder.build();

            // Subscribe using the handler
            js.subscribe(exArgs.subject, dispatcher, handler, false, so);

            // Wait for messages to arrive using the countdown latch. But don't wait forever.
            boolean countReachedZero = msgLatch.await(3, TimeUnit.SECONDS);

            System.out.printf("Received %d messages. Ignored %d messages. Timeout out ? %B.\n",
                    received.get(), ignored.get(), !countReachedZero) ;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}