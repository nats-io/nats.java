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
import io.nats.client.api.Mirror;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.time.Duration;
import java.util.List;

import static io.nats.client.support.JsonUtils.printFormatted;
import static io.nats.examples.jetstream.NatsJsUtils.publish;

/**
 * This example will demonstrate JetStream mirrors subscribing. .
 */
public class NatsJsMirrorSubUseCases {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsMirrorSubUseCases [-s server] [-strm stream] [-mir mirror] [-sub subject] [-dur durable]"
                    + "\n\nDefault Values:"
                    + "\n   [-strm] example-stream"
                    + "\n   [-mir]  example-mirror"
                    + "\n   [-sub]  example-subject"
                    + "\n   [-dur]  example-durable"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Mirror Subscription Use Cases", args, usageString)
                .defaultStream("example-stream")
                .defaultMirror("example-mirror")
                .defaultSubject("example-subject")
                .defaultDurable("example-durable")
                .build();

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // Create source stream
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(exArgs.stream)
                    .storageType(StorageType.Memory)
                    .subjects(exArgs.subject)
                    .build();
            jsm.addStream(sc);

            // the Mirror object needs the name of the source stream
            Mirror mirror = Mirror.builder().sourceName(exArgs.stream).build();

            // Now create our mirror stream.
            sc = StreamConfiguration.builder()
                    .name(exArgs.mirror)
                    .storageType(StorageType.Memory)
                    .mirror(mirror)
                    .build();
            StreamInfo si = jsm.addStream(sc);

            System.out.println("The mirror configuration...");
            printFormatted(si.getConfiguration());

            System.out.println("\nThe mirror info...");
            printFormatted(si.getMirrorInfo());

            // Send messages.
            publish(js, exArgs.subject, 5);

            System.out.println("\nMessages [pushed] from the stream, " + exArgs.stream + "...");
            JetStreamSubscription sub = js.subscribe(exArgs.subject);
            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            while (msg != null) {
                if (msg.isJetStream()) {
                    msg.ack();
                    System.out.println(msg + " [pushed] from " + msg.metaData().getStream());
                }
                msg = sub.nextMessage(Duration.ofSeconds(1));
            }

            // to read from the mirror, we must set the mirror as the source [stream]
            System.out.println("\nMessages [pushed] from the mirror, " + exArgs.mirror + "...");
            PushSubscribeOptions pushSo = PushSubscribeOptions.stream(exArgs.mirror);
            sub = js.subscribe(exArgs.subject, pushSo);
            msg = sub.nextMessage(Duration.ofSeconds(1));
            while (msg != null) {
                if (msg.isJetStream()) {
                    msg.ack();
                    System.out.println(msg + " [pushed] from " + msg.metaData().getStream());
                }
                msg = sub.nextMessage(Duration.ofSeconds(1));
            }

            System.out.println("\nMessages [pulled] from the mirror, " + exArgs.mirror + "...");
            PullSubscribeOptions pullSo = PullSubscribeOptions.builder()
                    .stream(exArgs.mirror)
                    .durable(exArgs.durable)
                    .build();

            sub = js.subscribe(exArgs.subject, pullSo);
            List<Message> messages = sub.fetch(5, Duration.ofSeconds(1));
            for (Message pulled : messages) {
                pulled.ack();
                System.out.println(pulled + " [pulled] from " + pulled.metaData().getStream());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
