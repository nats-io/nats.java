// Copyright 2015-2018 The NATS Authors
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

package io.nats.examples.benchmark;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;
import io.nats.examples.ExampleUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.nats.examples.ExampleUtils.uniqueEnough;

public class JsPublishInRoundsBench {
    static final String usageString =
            "\nUsage: java JsPublishInRoundsBench [serverURL] [help] " +
                    "[-m totalMessages] [-p payloadSize] [-r roundSize] [-o file|memory] [-c replicas] [-t stream] [-u subject]"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext";

    public static void main(String[] args) {

        // TO RUN WITH ARGS FROM IDE, ADD A LINE LIKE THESE
        // args = "-r 100 -p 128".split(" ");
        // args = "-r 250 -p 100".split(" ");
        // args = "myhost:4222 -r 1000 -p 64".split(" ");
        // args = "-r 500 -p 64 -o file -c 2".split(" ");
        // args = "help".split(" ");

        Arguments a = readArgs(args);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(a.server, true))) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamConfiguration.Builder builder = StreamConfiguration.builder()
                    .name(a.stream)
                    .storageType(a.file ? StorageType.File : StorageType.Memory)
                    .subjects(a.subject);

            if (a.replicas > 0) {
                builder.replicas(a.replicas);
            }

            StreamConfiguration sc = builder.build();

            try {
                jsm.addStream(sc);
                nc.flush(Duration.ofSeconds(5));
            }
            catch (Exception e) {
                System.out.println(e);
            }

            JetStream js = nc.jetStream();

            final Message m = NatsMessage.builder().subject(a.subject).data(new byte[a.payloadSize]).build();

            int sent = 0;
            int failed = 0;
            long totalElapsed = 0;
            while ((sent+failed) < a.totalMsgs) {
                long start = System.currentTimeMillis();
                List<CompletableFuture<PublishAck>> futures = new ArrayList<>();
                for (int x = 0; x < a.roundSize; x++) {
                    futures.add(js.publishAsync(m));
                }
                while (futures.size() > 0) {
                    List<CompletableFuture<PublishAck>> notDone = new ArrayList<>();
                    for (CompletableFuture<PublishAck> f : futures) {
                        if (f.isDone()) {
                            if (f.isCompletedExceptionally()) {
                                failed++;
                            } else {
                                sent++;
                            }
                        }
                        else {
                            notDone.add(f);
                        }
                    }
                    futures = notDone;
                }

                long roundElapsed = System.currentTimeMillis() - start;
                totalElapsed += roundElapsed;
                if ((sent+failed) % 10_000 == 0) {
                    long pubPerSec = totalElapsed == 0 ? 0 : (sent + failed) * 1000L / totalElapsed;
                    System.out.println("Sent " + sent + " | Failed " + failed + " | " + (sent + failed) + " | Round Elapsed " + roundElapsed + " | Total Elapsed " + totalElapsed + " | Pub/Sec " + pubPerSec);
                }
            }
            long pubPerSec = (sent+failed) * 1000L / totalElapsed;
            System.out.println("\nFINAL Sent " + sent + " | Failed " + failed + " | " + (sent+failed) + " | Total Elapsed " + totalElapsed + " | Pub/Sec " + pubPerSec);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Arguments {
        String server = Options.DEFAULT_URL;
        int totalMsgs = 1_000_000;
        int payloadSize = 128;
        int roundSize = 100;
        int replicas = 0;
        boolean file = false;
        String stream = "jspirb-strm-" + uniqueEnough();
        String subject = "jspirb-sub-" + uniqueEnough();
    }

    private static String defaultArgs() {
        Arguments a = new Arguments();
        return "\n\nDefault Arguments: " +
                "server='" + a.server + '\'' +
                ", totalMsgs=" + a.totalMsgs +
                ", payloadSize=" + a.payloadSize +
                ", roundSize=" + a.roundSize +
                ", replicas=" + a.replicas +
                ", storage=" + StorageType.Memory +
                ", stream='" + "jspirb-strm-<unique>" + '\'' +
                ", subject='" + "jspirb-sub-<unique>" + '\'';
    }

    private static Arguments readArgs(String[] args) {
        Arguments a = new Arguments();
        if (args.length > 0) {
            for (int x = 0; x < args.length; x++) {
                switch (args[x]) {
                    case "-m":
                        a.totalMsgs = Integer.parseInt(args[++x]);
                        break;
                    case "-p":
                        a.payloadSize = Integer.parseInt(args[++x]);
                        break;
                    case "-r":
                        a.roundSize = Integer.parseInt(args[++x]);
                        break;
                    case "-c":
                        a.replicas = Integer.parseInt(args[++x]);
                        break;
                    case "-o":
                        a.file = args[++x].equalsIgnoreCase("file");
                        break;
                    case "-t":
                        a.stream = args[++x];
                        break;
                    case "-u":
                        a.subject = args[++x];
                        break;
                    case "help":
                        System.err.println(usageString + defaultArgs());
                        System.exit(-1);
                    default:
                        a.server = args[x];
                        break;
                }
            }
        }
        return a;
    }
}