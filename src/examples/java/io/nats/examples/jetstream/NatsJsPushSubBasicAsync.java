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

import static io.nats.examples.jetstream.NatsJsUtils.exitIfStreamNotExists;

/**
 * This example will demonstrate JetStream basic asynchronous push subscribing with a handler.
 * Run NatsJsPub first to setup message data.
 */
public class NatsJsPushSubBasicAsync {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPushSubBasicAsync [-s server] [-strm stream] [-sub subject] [-mcnt msgCount] [-dur durable]"
                    + "\n\nDefault Values:"
                    + "\n   [-strm]    example-stream"
                    + "\n   [-sub]    example-subject"
                    + "\n   [-mcnt]  10"
                    + "\n\nRun Notes:"
                    + "\n   - durable is optional, durable behaves differently, try it by running this twice with durable set"
                    + "\n   - try msgCount less than or equal to or greater than the number of message you have to see different behavior"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Push Subscribe Basic Async", args, usageString)
            .defaultStream("example-stream")
            .defaultSubject("example-subject")
            .defaultMsgCount(100)
            // .defaultDurable("push-sub-basic-async-durable")
            .build();

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            // The stream (and data) must exist
            exitIfStreamNotExists(nc, exArgs.stream);

            // create a dispatcher without a default handler.
            Dispatcher dispatcher = nc.createDispatcher();

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            CountDownLatch msgLatch = new CountDownLatch(exArgs.msgCount);
            AtomicInteger received = new AtomicInteger();
            AtomicInteger ignored = new AtomicInteger();

            // create our message handler.
            MessageHandler handler = msg -> {
                if (msgLatch.getCount() == 0) {
                    ignored.incrementAndGet();
                    System.out.println("Message Ignored, latch count already reached "
                            + new String(msg.getData(), StandardCharsets.UTF_8));
                    msg.nak();
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
                        msg.getSubject(), new String(msg.getData(), StandardCharsets.UTF_8));
                    System.out.println("  " + msg.metaData());

                    // This example here's no auto-ack.
                    // The default Consumer Configuration AckPolicy is Explicit
                    // so we need to ack the message or it'll be redelivered.
                    msg.ack();

                    msgLatch.countDown();
                }
            };

            // Build our subscription options.
            // * A push subscription means the server will "push" us messages.
            // * Durable means the server will remember where we are if we use that name.
            // * Durable can by null or empty, the builder treats them the same.
            // * The stream name is not technically required. If it is not provided, the
            //   code building the subscription will look it up by making a request to the server.
            //   If you know the stream name, you might as well supply it and save a trip to the server.
            PushSubscribeOptions so = PushSubscribeOptions.builder()
                .stream(exArgs.stream)
                .durable(exArgs.durable) // it's okay if this is null, the builder handles it
                .build();

            // Subscribe using the handler
            js.subscribe(exArgs.subject, dispatcher, handler, false, so);

            // Wait for messages to arrive using the countdown latch. But don't wait forever.
            boolean countReachedZero = msgLatch.await(3, TimeUnit.SECONDS);

            System.out.printf("\nReceived %d messages. Ignored %d messages. Count Reached Zero ? %B.\n",
                    received.get(), ignored.get(), countReachedZero) ;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}