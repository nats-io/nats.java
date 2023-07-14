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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.examples.jetstream.simple.Utils.createOrReplaceStream;

/**
 * This example will demonstrate simplified next
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class QueueExample {
    private static final String STREAM = "q-stream";
    private static final String SUBJECT = "q-subject";
    private static final String CONSUMER_NAME = "q-consumer";
    private static final int MESSAGE_COUNT = 10000;
    private static final int CONSUMER_COUNT = 6;

    public static String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {
            JetStream js = nc.jetStream();
            createOrReplaceStream(nc.jetStreamManagement(), STREAM, SUBJECT);

            // get stream context and create a durable consumer
            // NOTE: It does not necessarily have to be a durable consumer,
            // it could be a named consumer with a long enough inactive threshold
            // to allow ConsumerContexts to be created from the name.
            StreamContext streamContext;
            try {
                streamContext = nc.streamContext(STREAM);
                streamContext.createOrUpdateConsumer(ConsumerConfiguration.builder().durable(CONSUMER_NAME).build());
            }
            catch (JetStreamApiException | IOException e) {
                // JetStreamApiException:
                //      the stream or consumer did not exist
                // IOException:
                //      likely a connection problem
                return;
            }

            // publish messages for the queue consumers to read
            for (int x = 1; x <= MESSAGE_COUNT; x++) {
                try {
                    js.publish(SUBJECT, ("message-" + x).getBytes());
                }
                catch (JetStreamApiException | IOException e) {
                    // JetStreamApiException:
                    //      The "publish" somehow was rejected by the server.
                    //      Unlikely w/o publish option expectations.
                    // IOException:
                    //      likely a connection problem
                    // InterruptedException:
                    //      developer interrupted this thread?
                    throw new RuntimeException(e);
                }
            }

            int readCount = MESSAGE_COUNT / CONSUMER_COUNT;
            System.out.println("Each of the " + MESSAGE_COUNT + " queue subscriptions will read about " + readCount + " messages.");

            CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);
            List<ConsumerHolder> holders = new ArrayList<>();
            for (int id = 1; id <= CONSUMER_COUNT; id++) {
                try {
                    if (id % 2 == 0) {
                        holders.add(new HandlerConsumerHolder(id, streamContext, latch));
                    }
                    else {
                        holders.add(new IterableConsumerHolder(id, streamContext, latch));
                    }
                }
                catch (JetStreamApiException e) {
                    // the stream or consumer did not exist
                    throw new RuntimeException(e);
                }
            }

            // wait for all messages to be received
            latch.await(20, TimeUnit.SECONDS);

            // report
            for (ConsumerHolder holder : holders) {
                holder.stop();
                holder.report();
            }

            System.out.println();

            // delete the stream since we are done with it.
            nc.jetStreamManagement().deleteStream(STREAM);
        }
        catch (IOException | InterruptedException | JetStreamApiException e) {
            // IOException:
            //      problem making the connection
            // InterruptedException:
            //      thread interruption in the body of the example
            // JetStreamApiException:
            //      the stream did not exist on delete
        }
    }

    static class HandlerConsumerHolder extends ConsumerHolder {
        MessageConsumer messageConsumer;

        public HandlerConsumerHolder(int id, StreamContext sc, CountDownLatch latch) throws JetStreamApiException, IOException {
            super(id, sc, latch);
            messageConsumer = consumerContext.consume(msg -> {
                thisReceived.incrementAndGet();
                latch.countDown();
                String data = new String(msg.getData(), StandardCharsets.US_ASCII);
                System.out.printf("Handler  # %d message # %d %s\n", id, thisReceived.get(), data);
                msg.ack();
            });
        }

        @Override
        public void stop() throws InterruptedException {
            messageConsumer.stop(1000);
        }
    }

    static class IterableConsumerHolder extends ConsumerHolder {
        IterableConsumer iterableConsumer;
        Thread t;
        CountDownLatch finished = new CountDownLatch(1);

        public IterableConsumerHolder(int id, StreamContext sc, CountDownLatch latch) throws JetStreamApiException, IOException {
            super(id, sc, latch);
            iterableConsumer = consumerContext.consume();
            t = new Thread(() -> {
                while (latch.getCount() > 0) {
                    try {
                        Message msg = iterableConsumer.nextMessage(1000);
                        if (msg != null) {
                            thisReceived.incrementAndGet();
                            latch.countDown();
                            String data = new String(msg.getData(), StandardCharsets.US_ASCII);
                            System.out.printf("Iterable # %d message # %d %s\n", id, thisReceived.get(), data);
                            msg.ack();
                        }
                    }
                    catch (Exception e) {
                    }
                }
                finished.countDown();
            });
            t.start();
        }

        @Override
        public void stop() throws InterruptedException {
            finished.await(2, TimeUnit.SECONDS); // ensures the next loop realized it could stop
        }
    }

    static abstract class ConsumerHolder {
        int id;
        ConsumerContext consumerContext;
        AtomicInteger thisReceived;
        CountDownLatch latch;

        public ConsumerHolder(int id, StreamContext sc, CountDownLatch latch) throws JetStreamApiException, IOException {
            this.id = id;
            thisReceived = new AtomicInteger();
            this.latch = latch;
            consumerContext = sc.consumerContext(CONSUMER_NAME);
        }

        public abstract void stop() throws InterruptedException;
        public void report() {
            System.out.printf("Instance # %d handled %d messages.\n", id, thisReceived.get());
        }
    }
}
