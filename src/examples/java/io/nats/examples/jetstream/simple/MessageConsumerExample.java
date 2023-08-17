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
import io.nats.client.api.ConsumerConfiguration;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.examples.jetstream.simple.Utils.Publisher;
import static io.nats.examples.jetstream.simple.Utils.createOrReplaceStream;

/**
 * This example will demonstrate simplified consume with a handler
 */
public class MessageConsumerExample {
    private static final String STREAM = "consume-stream";
    private static final String SUBJECT = "consume-subject";
    private static final String CONSUMER_NAME = "consume-consumer";
    private static final String MESSAGE_TEXT = "consume";
    private static final int STOP_COUNT = 500;
    private static final int REPORT_EVERY = 100;

    private static final String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {
            JetStream js = nc.jetStream();
            createOrReplaceStream(nc.jetStreamManagement(), STREAM, SUBJECT);

            System.out.println("Starting publish...");
            Publisher publisher = new Publisher(js, SUBJECT, MESSAGE_TEXT, 10);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            // get stream context, create consumer and get the consumer context
            StreamContext streamContext;
            ConsumerContext consumerContext;
            try {
                streamContext = nc.getStreamContext(STREAM);
                streamContext.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(CONSUMER_NAME).build());
                consumerContext = streamContext.getConsumerContext(CONSUMER_NAME);
            }
            catch (JetStreamApiException | IOException e) {
                // JetStreamApiException:
                //      the stream or consumer did not exist
                // IOException:
                //      likely a connection problem
                return;
            }

            CountDownLatch latch = new CountDownLatch(1);
            AtomicInteger atomicCount = new AtomicInteger();
            long start = System.nanoTime();
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
            try (MessageConsumer consumer = consumerContext.consume(handler)) {
                latch.await();

                // The consumer has at least 1 pull request active. When stop is called,
                // no more pull requests will be made, but messages already requested
                // will still come across the wire to the client.
                System.out.println("Stop the consumer...");
                consumer.stop();

                // wait until the consumer is finished
                while (!consumer.isFinished()) {
                    Thread.sleep(10);
                }
            }
            catch (JetStreamApiException | IOException e) {
                // JetStreamApiException:
                //      1. the stream or consumer did not exist
                //      2. api calls under the covers theoretically this could fail, but practically it won't.
                // IOException:
                //      likely a connection problem
                System.err.println("Exception should be handled properly, just exiting here.");
                System.exit(-1);
            }
            catch (Exception e) {
                // this is from the FetchConsumer being AutoCloseable, but should never be called
                // as work inside the close is already guarded by try/catch
                System.err.println("Exception should be handled properly, just exiting here.");
                System.exit(-1);
            }

            report("Final", start, atomicCount.get());

            publisher.stopPublishing(); // otherwise it will complain when the connection goes away
            pubThread.join();
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
