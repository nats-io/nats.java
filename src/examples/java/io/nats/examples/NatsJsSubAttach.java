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

/**
 * This jetstream example attaches to an existing consumer given a stream and consumer name.
 * The stream and consumer must exist in the NATS deployment.
 */
public class NatsJsSubAttach {

    static final String usageString = "\nUsage: java NatsJsSubAttach [-s server] [-poll #] -stream name -consumer name <subject> <msgCount>\n"
            + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleUtils.readSubscribeArgs(args, usageString);

        if (exArgs.stream == null || exArgs.consumer == null) {
            System.out.println("-stream <name> and -consumer <name> is required to attach.");
            System.exit(1);
        }
        System.out.printf("\nTrying to connect to %s, and listen to %s for %d messages.\n\n", exArgs.server,
                exArgs.subject, exArgs.msgCount);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            JetStream js = nc.jetStream();

            SubscribeOptions so = SubscribeOptions.builder().attach(exArgs.stream, exArgs.consumer).durable("sub-example").build();
            JetStreamSubscription sub = js.subscribe(exArgs.subject, so);
            nc.flush(Duration.ofSeconds(5));

            for(int i=1;i<=exArgs.msgCount;i++) {
                Message msg = sub.nextMessage(Duration.ofHours(1));

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
                    
                    // we assume the consumer requires explicit acking.  If the consumer
                    // has an ack policy of None or All, this would change.
                    msg.ack();
                }
            }
        }
        catch (Exception exp) {
            System.err.println(exp);
        }
    }
}