// Copyright 2023 The NATS Authors
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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.examples.jetstream.simple.Utils.*;

/**
 * This example will demonstrate simplified consume with a handler
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class ConsumeWithHandler {
    private static final String STREAM = "consume-handler-stream";
    private static final String SUBJECT = "consume-handler-subject";
    private static final String CONSUMER_NAME = "consume-handler-consumer";
    private static final String MESSAGE_TEXT = "consume-handler";
    private static final int STOP_COUNT = 500;
    private static final int REPORT_EVERY = 50;
    private static final int JITTER = 20;

    private static final String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // set's up the stream and create a durable consumer
            createOrReplaceStream(jsm, STREAM, SUBJECT);
            createConsumer(jsm, STREAM, CONSUMER_NAME);

            // Create the Consumer Context
            ConsumerContext consumerContext;
            try {
                consumerContext = js.getConsumerContext(STREAM, CONSUMER_NAME);
            }
            catch (JetStreamApiException | IOException e) {
                // JetStreamApiException:
                //      the stream or consumer did not exist
                // IOException:
                //      likely a connection problem
                return;
            }

            long start = System.nanoTime();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicInteger atomicCount = new AtomicInteger();
            MessageHandler handler = msg -> {
                msg.ack();
                int count = atomicCount.incrementAndGet();
                if (count % REPORT_EVERY == 0) {
                    report("Handler", start, count);
                }
                if (count == STOP_COUNT) {
                    latch.countDown();
                }
            };

            // create the consumer then use it
            SimpleConsumer consumer;
            try {
                consumer = consumerContext.consume(handler);
            }
            catch (JetStreamApiException | IOException e) {
                // JetStreamApiException:
                //      1. the stream or consumer did not exist
                //      2. api calls under the covers theoretically this could fail, but practically it won't.
                // IOException:
                //      likely a connection problem
                return;
            }

            Publisher publisher = new Publisher(js, SUBJECT, MESSAGE_TEXT, JITTER);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            latch.await();
            Thread.sleep(JITTER * 2); // allows more messages to come across
            publisher.stopPublishing();
            pubThread.join();

            System.out.println("Stop the consumer...");
            consumer.stop();

            Thread.sleep(250); // let consumer get messages post drain
            report("Final", start, atomicCount.get());
        }
        catch (IOException | InterruptedException ioe) {
            // IOException:
            //      problem making the connection
            // InterruptedException:
            //      thread interruption in the body of the example
        }
    }

    private static void report(String label, long start, int count) {
        long ms = (System.nanoTime() - start) / 1_000_000;
        System.out.println(label + ": Received " + count + " messages in " + ms + "ms.");
    }
}
