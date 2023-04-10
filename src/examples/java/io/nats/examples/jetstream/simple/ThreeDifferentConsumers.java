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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.examples.jetstream.simple.Utils.Publisher;
import static io.nats.examples.jetstream.simple.Utils.setupStream;

/**
 * This example will demonstrate all 3 simplified consumes running at the same time.
 * It just runs forever until you manually stop it.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public class ThreeDifferentConsumers {
    private static final String STREAM = "simple-stream";
    private static final String SUBJECT = "simple-subject";
    private static final int REPORT_EVERY = 100;
    private static final int JITTER = 30;

    // change this is you need to...
    public static String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {

            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            setupStream(jsm, STREAM, SUBJECT);

            String name1 = "next-" + NUID.nextGlobal();
            String name2 = "handle-" + NUID.nextGlobal();
            String name3 = "fetch-" + NUID.nextGlobal();

            jsm.addOrUpdateConsumer(STREAM, ConsumerConfiguration.builder().durable(name1).build());
            jsm.addOrUpdateConsumer(STREAM, ConsumerConfiguration.builder().durable(name2).build());
            jsm.addOrUpdateConsumer(STREAM, ConsumerConfiguration.builder().durable(name3).build());

            // Consumer[Context]
            ConsumerContext ctx1 = js.getConsumerContext(STREAM, name1);
            ConsumerContext ctx2 = js.getConsumerContext(STREAM, name2);
            ConsumerContext ctx3 = js.getConsumerContext(STREAM, name3);

            // create the consumer then use it
            ManualConsumer con1 = ctx1.consume();

            Thread con1Thread = new Thread(() -> {
                long mark = System.currentTimeMillis();
                int count = 0;
                long report = randomReportInterval();
                try {
                    while (true) {
                        Message msg = con1.nextMessage(1000);
                        if (msg != null) {
                            msg.ack();
                            ++count;
                            if (System.currentTimeMillis() - mark > report) {
                                System.out.println("Manual  " + count + " messages.");
                                mark = System.currentTimeMillis();
                                report = randomReportInterval();
                            }
                        }
                    }
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            con1Thread.start();

            Publisher publisher = new Publisher(js, SUBJECT, JITTER);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            Thread.sleep(1000); // just makes the consumers be reading different messages
            AtomicInteger atomicCount = new AtomicInteger();
            AtomicLong atomicMark = new AtomicLong(System.currentTimeMillis());
            AtomicLong atomicReport = new AtomicLong(randomReportInterval());

            MessageHandler handler = msg -> {
                msg.ack();
                int count = atomicCount.incrementAndGet();
                if (System.currentTimeMillis() - atomicMark.get() > atomicReport.get()) {
                    System.out.println("Handled " + count + " messages.");
                    atomicMark.set(System.currentTimeMillis());
                    atomicReport.set(randomReportInterval());
                }
            };
            SimpleConsumer con2 = ctx2.consume(handler);

            Thread.sleep(1000); // just makes the consumers be reading different messages
            Thread con2Thread = new Thread(() -> {
                int count = 0;
                long mark = System.currentTimeMillis();
                long report = randomReportInterval();
                try {
                    while (true) {
                        FetchConsumer fc = ctx3.fetch(REPORT_EVERY);
                        Message msg = fc.nextMessage();
                        while (msg != null) {
                            msg.ack();
                            ++count;
                            if (System.currentTimeMillis() - mark > report) {
                                System.out.println("Fetched " + count + " messages.");
                                mark = System.currentTimeMillis();
                                report = randomReportInterval();
                            }
                            msg = fc.nextMessage();
                        }
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            con2Thread.start();

            con2Thread.join(); // never ends so program runs until stopped.
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static long randomReportInterval() {
        return 1000 + ThreadLocalRandom.current().nextLong(1000);
    }
}
