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

import static io.nats.client.support.DebugUtil.printable;

public class NatsJsManage {

    static final String usageString = "\nUsage: java NatsJsManage [-s server]\n"
            + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL for user/pass/token authentication.\n";

    private static final String STREAM1 = "example-stream-1";
    private static final String STREAM2 = "example-stream-2";
    private static final String STRM1SUB1 = "strm1sub1";
    private static final String STRM1SUB2 = "strm1sub2";
    private static final String STRM2SUB1 = "strm2sub1";
    private static final String STRM2SUB2 = "strm2sub2";

    public static void main(String[] args) {
        ExampleArgs exArgs = new ExampleArgs(args, false, usageString);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, false))) {
            // Create a JetStream context.  This hangs off the original connection
            // allowing us to produce data to streams and consume data from
            // JetStream consumers.
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            action("Configure And Add Stream 1");
            StreamConfiguration sc1 = StreamConfiguration.builder()
                    .name(STREAM1)
                    .storageType(StreamConfiguration.StorageType.Memory)
                    .subjects(STRM1SUB1, STRM1SUB2).replicas(3)
                    .build();
            jsm.addStream(sc1);

            js.publish(STRM1SUB1, "blah blah".getBytes(StandardCharsets.UTF_8));
            StreamInfo si = jsm.streamInfo(STREAM1);
            printObject(si);

            action("Configure And Add Stream 2");
            StreamConfiguration sc2 = StreamConfiguration.builder()
                    .name(STREAM2)
                    .storageType(StreamConfiguration.StorageType.Memory)
                    .subjects(STRM2SUB1, STRM2SUB2)
                    .build();
            jsm.addStream(sc2);

            si = jsm.streamInfo(STREAM2);
            printObject(si);

            action("Delete Stream 2");
            jsm.deleteStream(STREAM2);

            action("Delete Non-Existent Stream");
            try {
                jsm.deleteStream(STREAM2);
            }
            catch (IllegalStateException ise) {
                System.out.println(ise.getMessage());
            }

            action("Update Non-Existent Stream ");
            try {
                StreamConfiguration non = StreamConfiguration.builder()
                        .name(STREAM2)
                        .storageType(StreamConfiguration.StorageType.Memory)
                        .subjects(STRM2SUB1, STRM2SUB2)
                        .build();
                jsm.updateStream(non);
            }
            catch (IllegalStateException ise) {
                System.out.println(ise.getMessage());
            }

            action("Configure And Add Consumer");
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .deliverSubject("strm1-deliver")
                    .durable("GQJ3IvWo")
                    .build();
            ConsumerInfo ci = jsm.addConsumer(STREAM1, cc);
            printObject(ci);
            si = jsm.streamInfo(STREAM1);
            printObject(si);

            action("Make And Use Subscription");
            PushSubscribeOptions so = PushSubscribeOptions.builder()
//                    .pullDirect(STREAM1, "GQJ3IvWo", 10)
//                    .configuration(STREAM1, cc)
                    .build();
            printObject(so);

            si = jsm.streamInfo(STREAM1);
            printObject(si);

            JetStreamSubscription sub = js.subscribe(STRM1SUB1, null, so);
            printObject(sub);

            action("List Consumers");
            ConsumerLister lister = jsm.getConsumers(STREAM1);
            printObject(lister);
        }
        catch (Exception exp) {
            action("EXCEPTION!!!");
            exp.printStackTrace();
        }
    }

    private static void action(String label) {
        System.out.println("================================================================================");
        System.out.println(label);
        System.out.println("--------------------------------------------------------------------------------");
    }

    public static void printObject(Object o) {
        System.out.println(printable(o.toString()) + "\n");
    }
}
