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
import java.util.concurrent.atomic.AtomicInteger;

public class NatsReply {

    static final String usageString =
            "\nUsage: java NatsReply [server] <subject> <msgCount>\n"
            + "\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
            + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
            + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
            + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) throws InterruptedException {
        args = "rr 1".split(" ");
        ExampleArgs exArgs = ExampleUtils.expectSubjectAndMsgCount(args, usageString);

        Thread t1 = new Thread(() -> extracted(exArgs, 1));
        Thread t2 = new Thread(() -> extracted(exArgs, 2));
        Thread t3 = new Thread(() -> extracted(exArgs, 3));
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
    }

    private static void extracted(ExampleArgs exArgs, int id) {
        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            CountDownLatch latch = new CountDownLatch(exArgs.msgCount); // dispatcher runs callback in another thread
            final AtomicInteger counter = new AtomicInteger(0);

            Dispatcher d = nc.createDispatcher((msg) -> {

                System.out.printf("\nMessage Received [%d]\n", counter.incrementAndGet());

                if (msg.hasHeaders()) {
                    System.out.println("  Headers:");
                    for (String key: msg.getHeaders().keySet()) {
                        for (String value : msg.getHeaders().get(key)) {
                            System.out.printf("    %s: %s\n", key, value);
                        }
                    }
                }

                System.out.printf("  Subject: %s\n  Data: %s\n",
                        msg.getSubject(),
                        new String(msg.getData(), StandardCharsets.UTF_8));
System.out.println("!!! " + id + " " + msg.getReplyTo());
                nc.publish(msg.getReplyTo(), ("" + id).getBytes());
                latch.countDown();
            });
            d.subscribe(exArgs.subject);

            nc.flush(Duration.ofSeconds(5));

            latch.await();

            nc.closeDispatcher(d); // This isn't required, closing the connection will do it
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}