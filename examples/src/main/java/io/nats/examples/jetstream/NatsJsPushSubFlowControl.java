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
import io.nats.client.impl.NatsMessage;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.examples.ExampleUtils.sleep;
import static io.nats.examples.jetstream.NatsJsUtils.createStreamExitWhenExists;

/**
 * This example will demonstrate JetStream listening to flow control messages through the Error Listener
 */
public class NatsJsPushSubFlowControl {
    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsPushSubFlowControl [-s server]"
            + "\n\nDefault Values:"
            + "\n   [-strm] fc-stream"
            + "\n   [-sub]  fc-subject"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Push Subscribe Using Flow Control", args, usageString)
            .defaultStream("fc-stream")
            .defaultSubject("fc-subject")
            .build();

        AtomicInteger flowControlMessagesProcessed = new AtomicInteger();

        ErrorListener el = new ErrorListener() {
            @Override
            public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
                System.out.printf("Flow Control Processed (%d), Connection: %d, Subject: %s, Source: %s\n",
                    flowControlMessagesProcessed.incrementAndGet(),
                    conn.getServerInfo().getClientId(), subject, source);
            }
        };

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, false, el, null))) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // Use the utility to create a stream stored in memory.
            createStreamExitWhenExists(jsm, exArgs.stream, exArgs.subject);

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // Set up the consumer configuration to have flowControl which
            // requires a idle heart beat time
            // IMPORTANT! 500 milliseconds is simply for this example
            // Generally this figure should be longer, otherwise the server
            // will be constantly sending heartbeats if there are no messages pending
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .flowControl(500)
                    .build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();

            // This is configured so the subscriber ends up being considered slow
            JetStreamSubscription sub = js.subscribe(exArgs.subject, pso);
            nc.flush(Duration.ofSeconds(5));

            // publish more message data than the subscriber will handle
            byte[] data = new byte[8192];
            for (int x = 1; x <= 1000; x++) {
                Message msg = NatsMessage.builder()
                        .subject(exArgs.subject)
                        .data(data)
                        .build();
                js.publish(msg);
            }

            // sleep to let the messages back up
            sleep(1000);


            int red = 0;
            Message msg = sub.nextMessage(Duration.ofSeconds(1)); // timeout can be Duration or millis
            while (msg != null) {
                msg.ack();
                ++red;
                msg = sub.nextMessage(1000); // timeout can be Duration or millis
            }

            System.out.println("\n" + red + " message(s) were received.");
            System.out.println(flowControlMessagesProcessed.get() + " flow control message(s) were processed.\n");

            sub.unsubscribe();
            nc.flush(Duration.ofSeconds(5));

            // delete the stream since we are done with it.
            jsm.deleteStream(exArgs.stream);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
