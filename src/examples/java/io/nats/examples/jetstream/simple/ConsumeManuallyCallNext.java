// Copyright 2020-2023 The NATS Authors
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

package io.nats.examples.jetstream.simple;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;

import java.time.Duration;

import static io.nats.examples.jetstream.simple.Utils.Publisher;
import static io.nats.examples.jetstream.simple.Utils.setupStream;

/**
 * This example will demonstrate simplified manual consume.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class ConsumeManuallyCallNext {
    private static final String STREAM = "simple-stream";
    private static final String SUBJECT = "simple-subject";
    private static final int STOP_COUNT = 500;
    private static final int REPORT_EVERY = 50;
    private static final int JITTER = 20;

    // change this is you need to...
    public static String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {

            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            setupStream(jsm, STREAM, SUBJECT);

            String name = "simple-consumer-" + NUID.nextGlobal();

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(name).build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(STREAM, name);

            // create the consumer then use it
            ManualConsumer consumer = consumerContext.consume();

            long start = System.nanoTime();
            Thread consumeThread = new Thread(() -> {
                int count = 0;
                try {
                    System.out.println("Starting main loop.");
                    while (count < STOP_COUNT) {
                        Message msg = consumer.nextMessage(1000);
                        if (msg != null) {
                            msg.ack();
                            if (++count % REPORT_EVERY == 0) {
                                report("Main Loop Running", start, count);
                            }
                        }
                    }
                    report("Main Loop Stopped", start, count);

                    System.out.println("Pausing for effect...allow more messages come across.");
                    Thread.sleep(JITTER * 2); // allows more messages to come across
                    consumer.drain(Duration.ofSeconds(1));

                    System.out.println("Starting post-drain loop.");
                    Message msg = consumer.nextMessage(1000);
                    while (msg != null) {
                        msg.ack();
                        report("Post Drain Loop Running", start, ++count);
                        msg = consumer.nextMessage(1000);
                    }
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                report("Done", start, count);
            });
            consumeThread.start();

            Publisher publisher = new Publisher(js, SUBJECT, JITTER);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            consumeThread.join();
            publisher.stop();
            pubThread.join();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void report(String label, long start, int count) {
        long ms = (System.nanoTime() - start) / 1_000_000;
        System.out.println(label + ": Received " + count + " messages in " + ms + "ms.");
    }
}
