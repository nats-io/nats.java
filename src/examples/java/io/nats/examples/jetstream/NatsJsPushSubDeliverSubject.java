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
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.time.Duration;

import static io.nats.examples.ExampleUtils.sleep;
import static io.nats.examples.ExampleUtils.uniqueEnough;
import static io.nats.examples.jetstream.NatsJsUtils.createStream;
import static io.nats.examples.jetstream.NatsJsUtils.publish;

/**
 * This example will demonstrate JetStream push subscribing with a delivery subject and how the delivery
 * subject can be used as a subject of a regular Nats message.
 */
public class NatsJsPushSubDeliverSubject {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPushSubDeliverSubject [-s server]"
                    + "\n\nRun Notes:"
                    + "\n   - THIS EXAMPLE IS NOT INTENDED TO BE CUSTOMIZED."
                    + "\n     Supply the [-s server] value if your server is not at localhost:4222"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder().build(args, usageString);
        String ue = uniqueEnough();
        String stream = "ds-strm-" + ue;
        String subjectNoAck = "ds-sub-noack" + ue;
        String subjectAck = "ds-sub-ack" + ue;
        String deliverNoAck = "ds-deliver-noack-" + ue;
        String deliverAck = "ds-deliver-ack-" + ue;

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            try {
                // creates a memory stream. We will clean it up at the end.
                createStream(jsm, stream, subjectNoAck, subjectAck);

                // Create our JetStream context to receive JetStream messages.
                JetStream js = nc.jetStream();

                // The server uses the delivery subject as both an inbox for a JetStream subscription
                // and as a regular nats messages subject.
                // BUT BE CAREFUL. THIS IS STILL A JETSTREAM SUBJECT AND ALL MESSAGES
                // ADHERE TO THE ACK POLICY

                // NoAck 1. Set up the noAck consumer / deliver subject configuration
                ConsumerConfiguration cc = ConsumerConfiguration.builder()
                        .ackPolicy(AckPolicy.None)
                        .ackWait(Duration.ofSeconds(1))
                        .build();

                PushSubscribeOptions pso = PushSubscribeOptions.builder()
                        .deliverSubject(deliverNoAck)
                        .configuration(cc)
                        .build();

                // NoAck 2. Set up the JetStream and regular subscriptions
                //          Notice the JetStream subscribes to the real subject
                //          and the regular subscribes to the delivery subject
                //          Order matters, you must do the JetStream subscribe first
                JetStreamSubscription jsSub = js.subscribe(subjectNoAck, pso);
                nc.flush(Duration.ofSeconds(5));
                Subscription regSub = nc.subscribe(deliverNoAck);

                // NoAck 3. Publish to the real subject
                publish(js, subjectNoAck, 1);

                // NoAck 4. Read the message with the js no ack subscription. No need to ack
                Message msg = jsSub.nextMessage(Duration.ofSeconds(1));
                printMessage("NoAck 4", msg);

                // NoAck 5. Read the message with the regular subscription on the
                //          no ack deliver subject. Since this message is a JetStream
                //          message we could ack. But we don't have to since the consumer
                //          was setup as AckPolicy None
                msg = regSub.nextMessage(Duration.ofSeconds(1));
                printMessage("NoAck 5", msg);

                // NoAck 6. Sleep longer than the ack wait period to check and make sure the
                //     message is not replayed
                sleep(1100);
                msg = regSub.nextMessage(Duration.ofSeconds(1));
                printMessage("NoAck 6, [Should be null]", msg);

                // Ack 1. Set up the Ack consumer / deliver subject configuration
                cc = ConsumerConfiguration.builder()
                        .ackPolicy(AckPolicy.Explicit)
                        .ackWait(Duration.ofSeconds(1))
                        .build();

                pso = PushSubscribeOptions.builder()
                        .deliverSubject(deliverAck)
                        .configuration(cc)
                        .build();

                // Ack 2. Set up the JetStream and regular subscriptions
                jsSub = js.subscribe(subjectAck, pso);
                nc.flush(Duration.ofSeconds(5));
                regSub = nc.subscribe(deliverAck);

                // Ack 3. Publish to the real subject
                publish(js, subjectAck, 1);

                // Ack 4. Read the message with the js no ack subscription. No need to ack
                msg = jsSub.nextMessage(Duration.ofSeconds(1));
                printMessage("Ack 4", msg);

                // Ack 5. Read the message with the regular subscription on the
                //        ack deliver subject.
                //        Even though it is read on a regular subscription
                //        it still is a JetStream message. Don't ack this time
                msg = regSub.nextMessage(Duration.ofSeconds(1));
                printMessage("Ack 5", msg);

                // Ack 6. Sleep longer than the ack wait period to check and
                //        see that the message is re-delivered. Ack this time.
                sleep(1100);
                msg = regSub.nextMessage(Duration.ofSeconds(1));
                msg.ack();
                printMessage("Ack 6", msg);

                // Ack 7. Sleep longer than the ack wait period. The message
                //        is not re-delivered this time
                sleep(1100);
                msg = regSub.nextMessage(Duration.ofSeconds(1));
                printMessage("Ack 7, [Should be null]", msg);
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

    private static void printMessage(String label, Message msg) {
        System.out.println(label);
        if (msg == null) {
            System.out.println("  Message: null");
        }
        else {
            System.out.println("  Message: " + msg);
            System.out.println("  JetStream: " + msg.isJetStream());
            System.out.println("  Meta: " + msg.metaData());
        }
    }
}
