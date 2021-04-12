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

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.impl.NatsMessage;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class NatsReqFuture {

    static final String usageString =
            "\nUsage: java -cp <classpath> NatsReq [-s server] [-r headerKey:headerValue]* <subject> <message>\n"
                    + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleUtils.expectSubjectAndMessage(args, usageString);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, false))) {

            String hdrNote = exArgs.hasHeaders() ? " with " + exArgs.headers.size() + " header(s)," : "";
            System.out.printf("\nRequesting '%s' on %s,%s server is %s\n\n", exArgs.message, exArgs.subject, hdrNote, exArgs.server);

            Future<Message> replyFuture = nc.request(NatsMessage.builder()
                    .subject(exArgs.subject)
                    .headers(exArgs.headers)
                    .data(exArgs.message, StandardCharsets.UTF_8)
                    .build());

            Message reply = replyFuture.get(5, TimeUnit.SECONDS);

            System.out.printf("\nReceived reply '%s' on subject '%s'\n\n",
                    new String(reply.getData(), StandardCharsets.UTF_8),
                    reply.getSubject());

            if (reply.hasHeaders()) {
                System.out.println("  Headers:");
                for (String key: reply.getHeaders().keySet()) {
                    for (String value : reply.getHeaders().get(key)) {
                        System.out.printf("    %s: %s\n", key, value);
                    }
                }
            }

        }
        catch (Exception e) {
            System.err.println(e);
        }
    }
}