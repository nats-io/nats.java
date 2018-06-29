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

package io.nats.examples.stan;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;

public class Stan {

    static final String usageString = "\nUsage: java Stan [server]"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "Stand listens on the subjects stan.time, stan.random and stan.exit.\n"
            + "Messages to stan.time are replied to with the current time, wherever stan is, in a UTF-8 string.\n"
            + "Messages to stan.random are replied to with a random integer, as UTF-8 string.\n"
            + "Messages to stan.exit will cause stan to go away, if they contain the data \"confirm\".\n" 
            + "The server name help will print this message.\n";

    public static void main(String args[]) {
        String server = "help";

        if (args.length == 1) {
            server = args[0];
        } else if (args.length == 0) {
            server = Options.DEFAULT_URL;
        }

        if (server.equals("help")) {
            usage();
            return;
        }

        try {
            SecureRandom random = new SecureRandom();
            CompletableFuture<Void> latch = new CompletableFuture<>();
            Options options = new Options.Builder().server(server).noReconnect().build();
            Connection nc = Nats.connect(options);

            Dispatcher d = nc.createDispatcher((msg) -> {

                String reply = "";
                boolean exit = false;

                switch (msg.getSubject()) {
                case "stan.time":
                    reply = LocalTime.now().toString();
                    break;
                case "stan.random":
                    reply = String.valueOf(random.nextInt());
                    break;
                case "stan.exit":
                    if ("confirm".equals(new String(msg.getData(), StandardCharsets.UTF_8))) {
                        reply = "exiting";
                        exit = true;
                    } else {
                        reply = "you have to confirm the exit";
                    }
                    break;
                }

                nc.publish(msg.getReplyTo(), reply.getBytes(StandardCharsets.UTF_8));

                if (exit) {
                    try {
                        nc.flush(Duration.ZERO);
                        latch.complete(null);
                    } catch (TimeoutException e) {
                        e.printStackTrace();
                    }
                }
            });
            d.subscribe("stan.time");
            d.subscribe("stan.random");
            d.subscribe("stan.exit");

            nc.flush(Duration.ZERO);

            System.out.println("Stan is listening...");
            latch.get();
            System.out.println("Stan is exiting ...");

            nc.close();

        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    static void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }
}