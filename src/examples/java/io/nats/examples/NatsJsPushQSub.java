// Copyright 2020 The NATS Authors
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

import io.nats.client.*;
import io.nats.client.impl.JetStreamApiException;
import io.nats.client.impl.NatsMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This example will demonstrate JetStream push subscribing. Run NatsJsPub first to setup message data.
 *
 * Usage: java NatsJsPushSub [-s server]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsPushQSub {

    // STREAM and SUBJECT are required.
    // DURABLE is optional (null), durable behaves differently, try it by running this twice with durable set
    // DELIVER is optional (null)
    // MSG_COUNT < 1 will just loop until there are no more messages
    static final String IDENT = "" + System.currentTimeMillis(); // so you can re-run without restarting the server
    static final String STREAM = "q-stream-" + IDENT;
    static final String SUBJECT = "q-subject" + IDENT;
    static final String QUEUE = "q-queue-" + IDENT;
    static final String DURABLE = "q-durable-" + IDENT;

    static final int MSG_COUNT = 100;
    static final int NO_OF_SUBSCRIBERS = 5;
    static final Duration TIMEOUT = Duration.ofMillis(500);

    public static void main(String[] args) {
        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(args, true))) {

            // Create the stream.
            NatsJsUtils.createOrUpdateStream(nc, STREAM, SUBJECT);

            // Create our JetStream context
            JetStream js = nc.jetStream();

            // Setup the subscribers
            // - the PushSubscribeOptions can be re-used since all the subscribers are the same
            // - use a concurrent integer to track all the messages received
            // - have a list of subscribers and threads so I can track them
            PushSubscribeOptions pso = PushSubscribeOptions.builder().durable(DURABLE).build();
            AtomicInteger allReceived = new AtomicInteger();
            List<QSubscriber> subscribers = new ArrayList<>();
            List<Thread> subThreads = new ArrayList<>();
            for (int x = 1; x <= NO_OF_SUBSCRIBERS; x++) {
                // setup the subscription
                JetStreamSubscription sub = js.subscribe(SUBJECT, QUEUE, pso);
                // create and track the runnable
                QSubscriber qs = new QSubscriber(x, allReceived, js, sub);
                subscribers.add(qs);
                // create, track and start the thread
                Thread t = new Thread(qs);
                subThreads.add(t);
                t.start();
            }
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // create and start the publishing
            Thread pubThread = new Thread(new QPublisher(js));
            pubThread.start();

            // wait for all threads to finish
            pubThread.join();
            for (Thread t : subThreads) {
                t.join();
            }

            // report
            for (QSubscriber qs : subscribers) {
                qs.report();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class QPublisher implements Runnable {
        JetStream js;

        public QPublisher(JetStream js) {
            this.js = js;
        }

        @Override
        public void run() {
            for (int x = 1; x <= MSG_COUNT; x++) {
                Message msg = NatsMessage.builder()
                        .subject(SUBJECT)
                        .data(("Data # " + x).getBytes(StandardCharsets.US_ASCII))
                        .build();
                try {
                    js.publish(msg);
                } catch (IOException | JetStreamApiException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class QSubscriber implements Runnable {
        int id;
        AtomicInteger thisReceived;
        AtomicInteger allReceived;
        JetStream js;
        JetStreamSubscription sub;

        public QSubscriber(int id, AtomicInteger allReceived, JetStream js, JetStreamSubscription sub) {
            this.id = id;
            this.thisReceived = new AtomicInteger();
            this.allReceived = allReceived;
            this.js = js;
            this.sub = sub;
        }

        public void report() {
            System.out.printf("QS # %d handled %d messages.\n", id, thisReceived.get());
        }

        @Override
        public void run() {
            while (allReceived.get() < MSG_COUNT) {
                try {
                    Message msg = sub.nextMessage(TIMEOUT);
                    while (msg != null) {
                        thisReceived.incrementAndGet();
                        allReceived.incrementAndGet();
                        System.out.printf("QS # %d message # %d %s\n",
                                id, thisReceived.get(), new String(msg.getData(), StandardCharsets.US_ASCII));
                        msg.ack();

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
