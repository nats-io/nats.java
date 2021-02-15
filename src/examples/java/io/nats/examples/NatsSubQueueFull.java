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
 *
 * Usage: java NatsSubQueueFull [server]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsSubQueueFull {

    // SUBJECT is required.
    // MSG_COUNT < 1 will just loop until there are no more messages
    static final String IDENT = "" + System.currentTimeMillis(); // so you can re-run without restarting the server
    static final String SUBJECT = "q-subject" + IDENT;
    static final String QUEUE = "q-queue-" + IDENT;

    static final int MSG_COUNT = 100;
    static final int NO_OF_SUBSCRIBERS = 5;
    static final Duration TIMEOUT = Duration.ofMillis(500);

    public static void main(String[] args) {
        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(args, true))) {

            // Setup the subscribers
            // - use a concurrent integer to track all the messages received
            // - have a list of subscribers and threads so I can track them
            AtomicInteger allReceived = new AtomicInteger();
            List<QueueSubscriber> subscribers = new ArrayList<>();
            List<Thread> subThreads = new ArrayList<>();
            for (int id = 1; id <= NO_OF_SUBSCRIBERS; id++) {
                // setup the subscription
                Subscription sub = nc.subscribe(SUBJECT, QUEUE);
                // create and track the runnable
                QueueSubscriber qs = new QueueSubscriber(id, sub, allReceived);
                subscribers.add(qs);
                // create, track and start the thread
                Thread t = new Thread(qs);
                subThreads.add(t);
                t.start();
            }
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // create and start the publishing
            Thread pubThread = new Thread(new Publisher(nc));
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

        public Publisher(Connection nc) {
            this.nc = nc;
        }

        @Override
        public void run() {
            for (int x = 1; x <= MSG_COUNT; x++) {
                nc.publish(SUBJECT, ("Data # " + x).getBytes(StandardCharsets.US_ASCII));
            }
        }
    }

    static class QueueSubscriber implements Runnable {
        int id;
        Subscription sub;
        AtomicInteger allReceived;
        AtomicInteger thisReceived;

        public QueueSubscriber(int id, Subscription sub, AtomicInteger allReceived) {
            this.id = id;
            this.sub = sub;
            this.allReceived = allReceived;
            this.thisReceived = new AtomicInteger();
        }

        public void report() {
            System.out.printf("SUB # %d handled %d messages.\n", id, thisReceived.get());
        }

        @Override
        public void run() {
            while (allReceived.get() < MSG_COUNT) {
                try {
                    Message msg = sub.nextMessage(TIMEOUT);
                    while (msg != null) {
                        thisReceived.incrementAndGet();
                        allReceived.incrementAndGet();
                        System.out.printf("SUB # %d message # %d %s\n",
                                id, thisReceived.get(), new String(msg.getData(), StandardCharsets.US_ASCII));
                        msg = sub.nextMessage(TIMEOUT);
                    }
                } catch (InterruptedException e) {
                    // just try again
                }
            }
            System.out.printf("QS # %d completed.\n", id);
        }
    }
}