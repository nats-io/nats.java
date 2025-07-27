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
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

public class NatsSubDispatch {

    static final String usageString =
            "\nUsage: java -cp <classpath> NatsDispatch [server] <subject> <msgCount>\n"
            + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleUtils.expectSubjectAndMsgCount(args, usageString);

        System.out.printf("Trying to connect to %s, and listen to %s for %d messages.\n\n", exArgs.server, exArgs.subject, exArgs.msgCount);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            CountDownLatch latch = new CountDownLatch(exArgs.msgCount); // dispatcher runs callback in another thread
            
            Dispatcher d = nc.createDispatcher(msg -> {
                System.out.printf("Received message \"%s\" on subject \"%s\"\n", 
                                        new String(msg.getData(), StandardCharsets.UTF_8), 
                                        msg.getSubject());
                latch.countDown();
            });
            d.subscribe(exArgs.subject);

            nc.flush(Duration.ZERO);

            latch.await();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
