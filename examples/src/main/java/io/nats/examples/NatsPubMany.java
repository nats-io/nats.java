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
import io.nats.client.Nats;
import io.nats.client.impl.NatsMessage;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static io.nats.examples.ExampleUtils.sleep;

/**
 * This example publishes many messages
 */
public class NatsPubMany {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsPubMany [-s server] [-r headerKey:headerValue]* [-mcnt msgCount] [-sub subject] [-m messageWords+]"
                    + "\n\nDefault Values:"
                    + "\n   [-mcnt msgCount]   10"
                    + "\n   [-sub subject]     example-subject"
                    + "\n   [-m messageWords+] hello"
                    + "\n\nRun Notes:"
                    + "\n   - durable is optional, durable behaves differently, try it by running this twice with durable set"
                    + "\n   - msgCount should be > 0"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Core Publish Many Messages", args, usageString)
                .defaultSubject("example-subject")
                .defaultMessage("hello")
                .defaultMsgCount(10)
                .build();

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, false))) {

            String hdrNote = exArgs.hasHeaders() ? " with " + exArgs.headers.size() + " header(s)" : "";
            System.out.printf("\nPublishing %d x '%s' to '%s'%s, server is %s\n\n", exArgs.msgCount, exArgs.message, exArgs.subject, hdrNote, exArgs.server);

            for (int i = 0; i < exArgs.msgCount; i++) {
                sleep(ThreadLocalRandom.current().nextLong(500)); // simulating real timing

                nc.publish(NatsMessage.builder()
                        .subject(exArgs.subject)
                        .headers(exArgs.headers)
                        .data(exArgs.message + " # " + i, StandardCharsets.UTF_8)
                        .build());

                nc.flush(Duration.ofSeconds(5));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
