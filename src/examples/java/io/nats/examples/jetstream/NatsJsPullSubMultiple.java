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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.examples.ExampleUtils.sleepRandom;

/**
 * This example will demonstrate JetStream multiple pull subscribers with the same durable names.
 * Messages are balanced like a push queue.
 */
public class NatsJsPullSubMultiple {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsPullSubMultiple [-s server] [-strm stream] [-sub subject] [-dur durable] [-mcnt msgCount] [-scnt subCount] [-pull pullSize]"
                    + "\n\nDefault Values:"
                    + "\n   [-strm stream]     pull-multi-stream"
                    + "\n   [-sub subject]     pull-multi-subject"
                    + "\n   [-dur durable]     pull-multi-durable"
                    + "\n   [-mcnt msgCount]   300"
                    + "\n   [-scnt subCount]   3"
                    + "\n   [-pull pullSize]   5"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL for user/pass/token authentication.\n";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("pull-multi-stream")
                .defaultSubject("pull-multi-subject")
                .defaultDurable("pull-multi-durable")
                .defaultMsgCount(300)
                .defaultSubCount(3)
                .defaultPullSize(5)
                .build(args, usageString);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server, true))) {

            // Create the stream.
            NatsJsUtils.createStream(nc, exArgs.stream, exArgs.subject);

            // Create our JetStream context
            JetStream js = nc.jetStream();

            // Setup the subscribers
            // - the PushSubscribeOptions can be re-used since all the subscribers are the same
            // - use a concurrent integer to track all the messages received
            // - have a list of subscribers and threads so I can track them
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(exArgs.durable)      // required
                    // .configuration(...) // if you want a custom io.nats.client.api.ConsumerConfiguration
                    .build();
            AtomicInteger allReceived = new AtomicInteger();
            List<JsPullSubscriber> subscribers = new ArrayList<>();
            List<Thread> subThreads = new ArrayList<>();
            for (int id = 1; id <= exArgs.subCount; id++) {
                // setup the subscription
                JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
                // create and track the runnable
                JsPullSubscriber qs = new JsPullSubscriber(id, exArgs, js, sub, allReceived);
                subscribers.add(qs);
                // create, track and start the thread
                Thread t = new Thread(qs);
                subThreads.add(t);
                t.start();
            }
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // create and start the publishing
            Thread pubThread = new Thread(new JsPublisher(js, exArgs));
            pubThread.start();

            // wait for all threads to finish
            pubThread.join();
            for (Thread t : subThreads) {
                t.join();
            }

            // report
            for (JsPullSubscriber qs : subscribers) {
                qs.report();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class JsPublisher implements Runnable {
        JetStream js;
        ExampleArgs exArgs;

        public JsPublisher(JetStream js, ExampleArgs exArgs) {
            this.js = js;
            this.exArgs = exArgs;
        }

        @Override
        public void run() {
            for (int x = 1; x <= exArgs.msgCount; x++) {
                try {
                    PublishAck pa = js.publish(exArgs.subject, ("Data # " + x).getBytes(StandardCharsets.US_ASCII));
                    System.out.println("PUB " + pa);
                } catch (IOException | JetStreamApiException e) {
                    // something pretty wrong here
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

    static class JsPullSubscriber implements Runnable {
        int id;
        ExampleArgs exArgs;
        JetStream js;
        JetStreamSubscription sub;
        AtomicInteger allReceived;
        AtomicInteger thisReceived;

        public JsPullSubscriber(int id, ExampleArgs exArgs, JetStream js, JetStreamSubscription sub, AtomicInteger allReceived) {
            this.id = id;
            this.exArgs = exArgs;
            this.js = js;
            this.sub = sub;
            this.allReceived = allReceived;
            this.thisReceived = new AtomicInteger();
        }

        public void report() {
            System.out.printf("PULL # %d handled %d messages.\n", id, thisReceived.get());
        }

        @Override
        public void run() {
            sub.pull(10);
            while (allReceived.get() < exArgs.msgCount) {
                sleepRandom(500);
                try {
                    Message msg = sub.nextMessage(Duration.ofMillis(500));
                    while (msg != null) {
                        thisReceived.incrementAndGet();
                        allReceived.incrementAndGet();
                        System.out.printf("PULL # %d message # %d %s\n",
                                id, thisReceived.get(), new String(msg.getData(), StandardCharsets.US_ASCII));
                        if (msg.isJetStream()) {
                            msg.ack();
                        }

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
