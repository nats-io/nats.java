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
import io.nats.client.Subscription;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class NatsSubQueue {

    static final String usageString =
            "\nUsage: java -cp <classpath> NatsSubQueue [server] <subject> <queue> <msgCount>\n"
            + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleUtils.expectSubjectQueueAndMsgCount(args, usageString);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {
            Subscription sub = nc.subscribe(exArgs.subject, exArgs.queue);
            nc.flush(Duration.ofSeconds(5));

            System.out.println();
            for(int i = 0; i < exArgs.msgCount; i++) {
                Message msg = sub.nextMessage(Duration.ofHours(1));
                System.out.printf("Received message \"%s\" on subject \"%s\"\n",
                                        new String(msg.getData(), StandardCharsets.UTF_8), 
                                        msg.getSubject());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
