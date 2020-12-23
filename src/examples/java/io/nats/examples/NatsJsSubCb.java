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
import java.util.concurrent.CountDownLatch;

public class NatsJsSubCb {

    static final String usageString = "\nUsage: java NatsJsSub [-s server] [--stream name] <subject> <msgCount>\n"
            + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleUtils.readSubscribeArgs(args, usageString);

        System.out.printf("\nTrying to connect to %s, and listen to %s for %d messages.\n\n", exArgs.server,
                exArgs.subject, exArgs.msgCount);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            // create a dispatcher without a default handler.
            Dispatcher d = nc.createDispatcher(null);

            // Create our jetstream context to subscribe to jetstream 
            // messages.
            JetStream js = nc.jetStream();

            // create a stream just in case it doesn't exist.
            if (exArgs.stream == null) {
                ExampleUtils.createTestStream(js, "test-stream", exArgs.subject);
            }            

            CountDownLatch msgLatch = new CountDownLatch(exArgs.msgCount);

            // create our message handler.
            MessageHandler handler = (Message msg) -> {

                System.out.printf("\nMessage Received:\n");

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

                    // If you don't disable auto ack mode NATS will automatically
                    // ack for you.  Otherwise, you'll need to ack, e.g.
                    // msg.ack();                    
                }
                msgLatch.countDown();
            };

            // Build our subscription options.  We'll create a durable subscription named
            // "sub-example".
            SubscribeOptions so = SubscribeOptions.builder().durable("sub-example").build();

            // Subscribe using the handler, and then wait for the requested number of 
            // messages to arrive using the countdown latch.
            js.subscribe(exArgs.subject, d, handler, so);

            msgLatch.await();
            System.out.printf("Received %d messages.  Done.\n", exArgs.msgCount);
        } catch (Exception exp) {
            System.err.println(exp);
        }
    }
}