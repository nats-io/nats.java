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
import java.util.concurrent.Future;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;

public class StanExit {

    static final String usageString =
            "\nUsage: java StanExit [server]"
            + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n";

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
            
            Options options = new Options.Builder().server(server).noReconnect().build();
            Connection nc = Nats.connect(options);
            Future<Message> replyFuture = nc.request("stan.exit", null);
            Message reply = replyFuture.get();

            System.out.printf("I asked stan to exit without confirmation, he replied \"%s\"\n", 
                                    new String(reply.getData(), StandardCharsets.UTF_8));

            replyFuture = nc.request("stan.exit", "confirm".getBytes(StandardCharsets.UTF_8));
            reply = replyFuture.get();

            System.out.printf("I asked stan to exit with confirmation, he replied \"%s\"\n", 
                                    new String(reply.getData(), StandardCharsets.UTF_8));

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