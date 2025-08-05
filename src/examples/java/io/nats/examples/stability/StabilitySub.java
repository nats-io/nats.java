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

import io.nats.client.*;
import io.nats.examples.benchmark.Utils;

import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;

public class StabilitySub {

    static final String usageString =
            "\nUsage: java -cp <classpath> StabilitySub [server] <subject>"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n";

    public static void main(String args[]) {
        String subject;
        String server;
        long nullCount = 0;
        long messageCount = 0;
        long payloadCount = 0;
        long restarts = 0;

        if (args.length == 2) {
            server = args[0];
            subject = args[1];
        } else if (args.length == 1) {
            server = Options.DEFAULT_URL;
            subject = args[0];
        } else {
            usage();
            return;
        }

        Instant start = Instant.now();

        System.out.println("Running stability subscriber for indefinite test, ctrl-c to cancel...\n");

        while (true) {
            try {
                Options options = new Options.Builder().server(server).noReconnect().build();
                Connection nc = Nats.connect(options);
                Subscription sub = nc.subscribe(subject);

                try {
                    while(true) { // receive as long as we can
                        Message msg = sub.nextMessage(Duration.ofHours(1));

                        if (msg == null) {
                            nullCount++;
                        } else if (msg.getData() != null) {
                            payloadCount += msg.getData().length;
                        }

                        messageCount++;

                        if (messageCount != 0 && messageCount % 100_000 == 0) {
                            Instant finish = Instant.now();
                            System.out.printf("Running for %s\n", Duration.between(start, finish).toString()
                                                                .substring(2)
                                                                .replaceAll("(\\d[HMS])(?!$)", "$1 ")
                                                                .toLowerCase());
                            System.out.printf("Received %s messages.\n", NumberFormat.getIntegerInstance().format(messageCount));
                            System.out.printf("Received %s payload bytes.\n", Utils.humanBytes(payloadCount));
                            System.out.printf("Received %s null messages.\n", NumberFormat.getIntegerInstance().format(nullCount));
                            System.out.printf("Restarted %s times.\n", NumberFormat.getIntegerInstance().format(restarts));
                            System.out.printf("Current memory usage is %s / %s / %s free/total/max\n", 
                                                                        Utils.humanBytes(Runtime.getRuntime().freeMemory()),
                                                                        Utils.humanBytes(Runtime.getRuntime().totalMemory()),
                                                                        Utils.humanBytes(Runtime.getRuntime().maxMemory()));
                            System.out.println();
                        }
                    }
                } catch (Exception exp) {
                    System.out.println("Exception from running connection, creating a new one.");
                    exp.printStackTrace();
                    System.out.println("Reconnecting...");
                    System.out.println();
                }

                restarts++;

            } catch (Exception exp) {
                System.out.println("Exception connecting, exiting...");
                exp.printStackTrace();
                System.exit(-1);
            }
        }
    }

    static void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }
}