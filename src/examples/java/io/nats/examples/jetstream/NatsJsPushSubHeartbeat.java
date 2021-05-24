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
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.time.Duration;

import static io.nats.examples.ExampleUtils.uniqueEnough;
import static io.nats.examples.jetstream.NatsJsUtils.createStream;

/**
 * This example will demonstrate receiving heartbeats.
 */
public class NatsJsPushSubHeartbeat {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPushSubHeartbeat [-s server]"
                    + "\n\nRun Notes:"
                    + "\n   - THIS EXAMPLE IS NOT INTENDED TO BE CUSTOMIZED."
                    + "\n     Supply the [-s server] value if your server is not at localhost:4222"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder().build(args, usageString);
        String stream = "hb-strm-" + uniqueEnough();
        String subject = "hb-sub-" + uniqueEnough();

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            try {
                // creates a memory stream. We will clean it up at the end.
                createStream(jsm, stream, subject);

                // Create our JetStream context to receive JetStream messages.
                JetStream js = nc.jetStream();

                // set the idle heartbeat value so the server knows to send us heartbeat status messages.
                ConsumerConfiguration cc = ConsumerConfiguration.builder()
                        .idleHeartbeat(Duration.ofSeconds(1))
                        .deliverPolicy(DeliverPolicy.New)
                        .build();
                PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();

                // Subscribe
                JetStreamSubscription sub = js.subscribe(subject, pso);
                nc.flush(Duration.ofSeconds(5));

                // There are no messages, so we will get a heartbeat instead
                boolean waitingForHeartbeat = true;
                while (waitingForHeartbeat) {
                    Message msg = sub.nextMessage(Duration.ofSeconds(1));
                    if (msg != null) {
                        // -------------------------------------------------------------------
                        //  A HEARTBEAT MESSAGE IS A STATUS MESSAGE
                        // The Status object has a helper method `isHeartbeat()`
                        // -------------------------------------------------------------------
                        if (msg.isStatusMessage() && msg.getStatus().isHeartbeat()) {
                            System.out.println("STATUS: " + msg.getStatus().getMessage());
                            waitingForHeartbeat = false;
                        }
                    }
                }

                sub.unsubscribe();
                nc.flush(Duration.ofSeconds(5));
            }
            finally {
                // be a good citizen and remove the example stream
                jsm.deleteStream(stream);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
