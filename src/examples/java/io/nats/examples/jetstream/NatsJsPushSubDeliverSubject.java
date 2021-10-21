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
import static io.nats.examples.jetstream.NatsJsUtils.createStreamExitWhenExists;
import static io.nats.examples.jetstream.NatsJsUtils.publish;

/**
 * This example will demonstrate JetStream push subscribing with a delivery subject
 * and how the delivery subject can be used as a subject of a core Nats message.
 */
public class NatsJsPushSubDeliverSubject {

    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsPushSubDeliverSubject [-s server] [-strm stream] [-sub subject-prefix] [-deliver deliver-prefix]"
            + "\n\nDefault Values:"
            + "\n   [-strm]    ds-stream"
            + "\n   [-sub]     ds-subject-"
            + "\n   [-deliver] ds-target-"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Push Subscribe With Deliver Subject", args, usageString)
            .defaultStream("ds-stream")
            .defaultSubject("ds-subject-")
            .defaultDeliverSubject("ds-target-")
            .build();

        String subjectNoAck = exArgs.subject + "noack";
        String subjectAck = exArgs.subject + "ack";
        String deliverNoAck = exArgs.deliverSubject + "noack";
        String deliverAck = exArgs.deliverSubject + "ack";

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            // Create a JetStreamManagement context.
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Use the utility to create a stream stored in memory.
            createStreamExitWhenExists(jsm, exArgs.stream, subjectNoAck, subjectAck);

            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // The server uses the delivery subject as both an inbox for a JetStream subscription
            // and as a core nats messages subject.
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

            // NoAck 2. Set up the JetStream and core subscriptions
            //          Notice the JetStream subscribes to the real subject
            //          and the core subscribes to the delivery subject
            //          Order matters, you must do the JetStream subscribe first
            //          But you also must make sure the core sub is made
            //          before messages are published
            JetStreamSubscription jsSub = js.subscribe(subjectNoAck, pso);
            nc.flush(Duration.ofSeconds(5));
            Subscription coreSub = nc.subscribe(deliverNoAck);

            // NoAck 3. Publish to the real subject
            publish(js, subjectNoAck, "A", 1);

            // NoAck 4. Read the message with the js no ack subscription. No need to ack
            Message msg = jsSub.nextMessage(Duration.ofSeconds(1));
            printMessage("\nNoAck 4. Read w/JetStream sub", msg);

            // NoAck 5. Read the message with the core subscription on the
            //          no ack deliver subject. Since this message is a JetStream
            //          message we could ack. But we don't have to since the consumer
            //          was setup as AckPolicy None
            msg = coreSub.nextMessage(Duration.ofSeconds(1));
            printMessage("NoAck 5. Read w/core sub", msg);

            // NoAck 6. Sleep longer than the ack wait period to check and make sure the
            //     message is not replayed
            sleep(1100);
            msg = coreSub.nextMessage(Duration.ofSeconds(1));
            printMessage("NoAck 6. Read w/core sub.\nAck Policy is none so no replay even though message was not Ack'd.\nmessage should be null", msg);

            // Ack 1. Set up the Ack consumer / deliver subject configuration
            cc = ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.Explicit)
                    .ackWait(Duration.ofSeconds(1))
                    .build();

            pso = PushSubscribeOptions.builder()
                    .deliverSubject(deliverAck)
                    .configuration(cc)
                    .build();

            // Ack 2. Set up the JetStream and core subscriptions
            jsSub = js.subscribe(subjectAck, pso);
            nc.flush(Duration.ofSeconds(5));
            coreSub = nc.subscribe(deliverAck);

            // Ack 3. Publish to the real subject
            publish(js, subjectAck, "B", 1);

            // Ack 4. Read the message with the js no ack subscription. No need to ack
            msg = jsSub.nextMessage(Duration.ofSeconds(1));
            printMessage("\nAck 4. Read w/JetStream sub", msg);

            // Ack 5. Read the message with the core subscription on the
            //        ack deliver subject.
            //        Even though it is read on a core subscription
            //        it still is a JetStream message. Don't ack this time
            msg = coreSub.nextMessage(Duration.ofSeconds(1));
            printMessage("Ack 5. Read w/core sub", msg);

            // Ack 6. Sleep longer than the ack wait period to check and
            //        see that the message is re-delivered. Ack this time.
            sleep(1100);
            msg = coreSub.nextMessage(Duration.ofSeconds(1));
            msg.ack();
            printMessage("Ack 6. Read w/core sub.\nWasn't Ack'd after step 'Ack 5.' so message was replayed.", msg);

            // Ack 7. Sleep longer than the ack wait period. The message
            //        is not re-delivered this time
            sleep(1100);
            msg = coreSub.nextMessage(Duration.ofSeconds(1));
            printMessage("Ack 7. Read w/core sub.\nMessage received by core sub in step 'Ack 6.' was JetStream so it was Ack'd and therefore not replayed.\nMessage should be null", msg);

            // delete the stream since we are done with it.
            jsm.deleteStream(exArgs.stream);
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
        System.out.println();
    }
}
