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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This example will demonstrate queue subscribing.
 */
public class NatsSubQueueFull {

    static final String usageString =
            "\nUsage: java -cp <classpath> NatsSubQueueFull [-s server] [-sub subject] [-q queue] [-mcnt msgCount] [-scnt subCount]"
                    + "\n\nDefault Values:"
                    + "\n   [-sub subject]   q-subject"
                    + "\n   [-q queue]       q-queue"
                    + "\n   [-mcnt msgCount] 100"
                    + "\n   [-scnt subCount] 5"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultSubject("q-subject")
                .defaultQueue("q-queue")
                .defaultMsgCount(100)
                .defaultSubCount(5)
                .build(args, usageString);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            // Setup the subscribers
            // - use a concurrent integer to track all the messages received
            // - have a list of subscribers and threads so I can track them
            AtomicInteger allReceived = new AtomicInteger();
            List<QueueSubscriber> subscribers = new ArrayList<>();
            List<Thread> subThreads = new ArrayList<>();
            for (int id = 1; id <= exArgs.subCount; id++) {
                // setup the subscription
                Subscription sub = nc.subscribe(exArgs.subject, exArgs.queue);
                // create and track the runnable
                QueueSubscriber qs = new QueueSubscriber(id, exArgs.msgCount, sub, allReceived);
                subscribers.add(qs);
                // create, track and start the thread
                Thread t = new Thread(qs);
                subThreads.add(t);
                t.start();
            }
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // create and start the publishing
            Thread pubThread = new Thread(new Publisher(nc, exArgs.subject, exArgs.msgCount));
            pubThread.start();

            // wait for all threads to finish
            pubThread.join();
            for (Thread t : subThreads) {
                t.join();
            }

            // report
            for (QueueSubscriber qs : subscribers) {
                qs.report();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Publisher implements Runnable {
        Connection nc;
        String subject;
        int msgCount;

        public Publisher(Connection nc, String subject, int msgCount) {
            this.nc = nc;
            this.subject = subject;
            this.msgCount = msgCount;
        }

        @Override
        public void run() {
            for (int x = 1; x <= msgCount; x++) {
                nc.publish(subject, ("Data # " + x).getBytes(StandardCharsets.US_ASCII));
            }
        }
    }

    static class QueueSubscriber implements Runnable {
        int id;
        int msgCount;
        Subscription sub;
        AtomicInteger allReceived;
        AtomicInteger thisReceived;

        public QueueSubscriber(int id, int msgCount, Subscription sub, AtomicInteger allReceived) {
            this.id = id;
            this.msgCount = msgCount;
            this.sub = sub;
            this.allReceived = allReceived;
            this.thisReceived = new AtomicInteger();
        }

        public void report() {
            System.out.printf("SUB # %d handled %d messages.\n", id, thisReceived.get());
        }

        @Override
        public void run() {
            while (allReceived.get() < msgCount) {
                try {
                    Message msg = sub.nextMessage(Duration.ofMillis(500));
                    while (msg != null) {
                        thisReceived.incrementAndGet();
                        allReceived.incrementAndGet();
                        System.out.printf("SUB # %d message # %d %s\n",
                                id, thisReceived.get(), new String(msg.getData(), StandardCharsets.US_ASCII));
                        msg = sub.nextMessage(Duration.ofMillis(500));
                    }
                } catch (InterruptedException e) {
                    // just try again
                }
            }
            System.out.printf("QS # %d completed.\n", id);
        }
    }
}