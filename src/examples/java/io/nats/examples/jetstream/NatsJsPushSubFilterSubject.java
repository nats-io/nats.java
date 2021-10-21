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

import static io.nats.examples.jetstream.NatsJsUtils.createStreamExitWhenExists;
import static io.nats.examples.jetstream.NatsJsUtils.publish;

/**
 * This example will demonstrate JetStream push subscribing with a filter on the subjects.
 */
public class NatsJsPushSubFilterSubject {
    static final String usageString =
        "\nUsage: java -cp <classpath> NatsJsPushSubFilterSubject [-s server] [-strm stream] [-sub subject-prefix]"
            + "\n\nDefault Values:"
            + "\n   [-strm] fs-stream"
            + "\n   [-sub]  fs-subject"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Push Subscribe with Filter Subject", args, usageString)
            .defaultStream("fs-stream")
            .defaultSubject("fs-subject")
            .build();

        String subjectWild = exArgs.subject + ".*";
        String subjectA = exArgs.subject + ".A";
        String subjectB = exArgs.subject + ".B";

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {
            // Create a JetStreamManagement context.
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Use the utility to create a stream stored in memory.
            createStreamExitWhenExists(jsm, exArgs.stream, subjectWild);

            // Create our JetStream context to publish and receive JetStream messages.
            JetStream js = nc.jetStream();

            publish(js, subjectA, 1);
            publish(js, subjectB, 1);
            publish(js, subjectA, 1);
            publish(js, subjectB, 1);

            // 1. create a subscription that subscribes to the wildcard subject
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.None) // don't want to worry about acking messages.
                    .build();

            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();

            JetStreamSubscription sub = js.subscribe(subjectWild, pso);
            nc.flush(Duration.ofSeconds(5));

            Message m = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("\n1A1. Message should be from '" + subjectA + "', Sequence #1. " +
                "It was: '" + m.getSubject() + "', Seq #" + m.metaData().streamSequence());

            m = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("1B2. Message should be from '" + subjectB + "', Sequence #2. " +
                "It was: '" + m.getSubject() + "', Seq #" + m.metaData().streamSequence());

            m = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("1A3. Message should be from '" + subjectA + "', Sequence #3. " +
                "It was: '" + m.getSubject() + "', Seq #" + m.metaData().streamSequence());

            m = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("1B4. Message should be from '" + subjectB + "', Sequence #4. " +
                "It was: '" + m.getSubject() + "', Seq #" + m.metaData().streamSequence());

            // 2. create a subscription that subscribes only to the A subject
            cc = ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.None) // don't want to worry about acking messages.
                    .filterSubject(subjectA)
                    .build();

            pso = PushSubscribeOptions.builder().configuration(cc).build();

            sub = js.subscribe(subjectWild, pso);
            nc.flush(Duration.ofSeconds(5));

            m = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("\n2A1. Message should be from '" + subjectA + "', Sequence #1. " +
                "It was: '" + m.getSubject() + "', Seq #" + m.metaData().streamSequence());

            m = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("2A3. Message should be from '" + subjectA + "', Sequence #3. " +
                "It was: '" + m.getSubject() + "', Seq #" + m.metaData().streamSequence());

            m = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("2x. Message should be null: " + m);

            // 3. create a subscription that subscribes only to the A subject
            cc = ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.None) // don't want to worry about acking messages.
                    .filterSubject(subjectB)
                    .build();

            pso = PushSubscribeOptions.builder().configuration(cc).build();

            sub = js.subscribe(subjectWild, pso);
            nc.flush(Duration.ofSeconds(5));

            m = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("\n3A2. Message should be from '" + subjectB + "', Sequence #2. " +
                "It was: '" + m.getSubject() + "', Seq #" + m.metaData().streamSequence());

            m = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("3A4. Message should be from '" + subjectB + "', Sequence #4. " +
                "It was: '" + m.getSubject() + "', Seq #" + m.metaData().streamSequence());

            m = sub.nextMessage(Duration.ofSeconds(1));
            System.out.println("3x. Message should be null: " + m);

            System.out.println();

            // delete the stream since we are done with it.
            jsm.deleteStream(exArgs.stream);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
