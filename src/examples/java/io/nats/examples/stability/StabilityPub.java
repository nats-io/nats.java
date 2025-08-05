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

package io.nats.examples.stability;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.examples.benchmark.Utils;

import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;

public class StabilityPub {

    static final String usageString =
            "\nUsage: java -cp <classpath> StabilityPub [server] <subject> <msgSize>"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n";

    public static void main(String args[]) {
        String subject;
        String server;
        int msgSize;
        long messageCount = 0;
        long payloadCount = 0;

        if (args.length == 3) {
            server = args[0];
            subject = args[1];
            msgSize = Integer.parseInt(args[2]);
        } else if (args.length == 2) {
            server = Options.DEFAULT_URL;
            subject = args[0];
            msgSize = Integer.parseInt(args[1]);
        } else {
            usage();
            return;
        }

        try {
            Options options = new Options.Builder().server(server).noReconnect().build();
            Connection nc = Nats.connect(options);
            Instant start = Instant.now();

            byte[] payload = new byte[msgSize];
            
            System.out.println("Running stability publisher for indefinite test, ctrl-c to cancel...\n");

            while (true) {
                nc.publish(subject, payload);

                payloadCount += msgSize;
                messageCount++;

                // This is a long running test, we are going to try for a message rate around
                // 10,000/sec not a lot but ok for wifi/slow consumers, the main point
                // is to run a long time and be able to watch memory/stability over time
                if (messageCount != 0 && messageCount % 1_000 == 0) {
                    nc.flush(Duration.ofSeconds(30));
                    try {
                        Thread.sleep(100);
                    } catch(Exception exp) {
                        // ignore it
                    }
                }

                if (messageCount != 0 && messageCount % 100_000 == 0) {
                    Instant finish = Instant.now();
                    System.out.printf("Running for %s\n", Duration.between(start, finish).toString()
                                                        .substring(2)
                                                        .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                                                        .toLowerCase());
                    System.out.printf("Sent %s messages.\n", NumberFormat.getIntegerInstance().format(messageCount));
                    System.out.printf("Sent %s payload bytes.\n", Utils.humanBytes(payloadCount));
                    System.out.printf("Current memory usage is %s / %s / %s free/total/max\n", 
                                                                Utils.humanBytes(Runtime.getRuntime().freeMemory()),
                                                                Utils.humanBytes(Runtime.getRuntime().totalMemory()),
                                                                Utils.humanBytes(Runtime.getRuntime().maxMemory()));
                    System.out.println();
                }
            }

        } catch (Exception exp) {
            exp.printStackTrace();
            System.exit(-1);
        }
    }

    static void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }
}