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

package io.nats.examples;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;

public class NatsReq {

    static final String usageString =
            "\nUsage: java NatsReq [server] <subject> <text message>\n"
            + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String args[]) {
        String subject;
        String message;
        String server;

        if (args.length == 3) {
            server = args[0];
            subject = args[1];
            message = args[2];
        } else if (args.length == 2) {
            server = Options.DEFAULT_URL;
            subject = args[0];
            message = args[1];
        } else {
            usage();
            return;
        }

        try {
            Connection nc = Nats.connect(ExampleUtils.createExampleOptions(server, false));
            Future<Message> replyFuture = nc.request(subject, message.getBytes(StandardCharsets.UTF_8));
            Message reply = replyFuture.get(5, TimeUnit.SECONDS);

            System.out.println();
            System.out.printf("Received reply \"%s\" on subject \"%s\"\n", 
                                    new String(reply.getData(), StandardCharsets.UTF_8), 
                                    reply.getSubject());
            System.out.println();

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