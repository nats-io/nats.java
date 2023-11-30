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
import io.nats.client.api.*;
import io.nats.client.impl.NatsMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This example will demonstrate simplified fetch that is resilient
 * HOW TO TEST
 * 1. Set up and run a simple cluster. See https://github.com/nats-io/java-nats-examples/tree/main/example-cluster-config
 * 2. Run this program, watch the output for some time
 * 3. Kill the server that is the leader, let it stay down for a short time or a long time
 * 4. See the output showing things aren't running.
 * 5. Bring the killed server back up and watch the output showing recovery.
 */
public class FetchResilientExample {
    static final String STREAM = "fetch-resilient-stream";
    static final String SUBJECT = "fetch-resilient-subject";
    static final String CONSUMER_NAME = "fetch-resilient-consumer";
    static final String MESSAGE_TEXT_PREFIX = "fetch-resilient-message";
    static final int FETCH_SIZE = 500;
    static final int PUBLISH_DELAY = 100;
    static final int CONSUME_REPORT_FREQUENCY = 2000;
    static final int PRINT_REPORT_FREQUENCY = 5000;
    static final int MISC_REPORT_FREQUENCY = 30000;

    static String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER)
            .connectionListener((c, t) -> {
                report("Connection: " + c.getServerInfo().getPort() + " " + t);
            })
            .build();

        try (Connection nc = Nats.connect(options)) {
            final JetStreamManagement jsm = nc.jetStreamManagement();

            setupStream(jsm, STREAM, SUBJECT);
            Thread miscReportingThread = getMiscReportingThread(jsm);
            miscReportingThread.start();

            JetStream js = nc.jetStream();

            // simulating a publisher somewhere else.
            ResilientPublisher rp = new ResilientPublisher(js, SUBJECT, MESSAGE_TEXT_PREFIX, Integer.MAX_VALUE, PUBLISH_DELAY);
            Thread pubThread = new Thread(rp);
            pubThread.start();

            StreamContext sc = js.getStreamContext(STREAM);
            ConsumerContext cc = sc.createOrUpdateConsumer(
                ConsumerConfiguration.builder()
                    .durable(CONSUMER_NAME)
                    .filterSubject(SUBJECT)
                    .build());

            ExampleFetchConsumer example = new ExampleFetchConsumer(cc);
            Thread consumeThread = new Thread(example);
            consumeThread.start();

            consumeThread.join(); // just letting this run
        }
        catch (IOException ioe) {
            // problem making the connection or
        }
        catch (InterruptedException e) {
            // thread interruption in the body of the example
        }
        catch (JetStreamApiException e) {
            // some api exception
        }
    }

    static class ExampleFetchConsumer implements Runnable {
        public AtomicBoolean keepGoing = new AtomicBoolean(true);
        ConsumerContext cc;
        long reportAt = 0;

        public ExampleFetchConsumer(ConsumerContext cc) {
            this.cc = cc;
        }

        @Override
        public void run() {
            long lastReadStreamSeq = 0;
            while (keepGoing.get()) {
                try (FetchConsumer fc = cc.fetchMessages(FETCH_SIZE)) {
                    Message m = fc.nextMessage();
                    while (m != null) {
                        lastReadStreamSeq = m.metaData().streamSequence();
                        if (System.currentTimeMillis() > reportAt) {
                            report("Last Read Sequence: " + lastReadStreamSeq);
                            reportAt = System.currentTimeMillis() + CONSUME_REPORT_FREQUENCY;
                        }
                        m.ack();
                        m = fc.nextMessage();
                    }
                }
                catch (Exception e) {
                    // do we care if the autocloseable FetchConsumer errors on close?
                    // probably not, but maybe log it.
                }

                // simulating some work to be done between fetches
                try {
                    if (System.currentTimeMillis() > reportAt) {
                        report("Last Read Sequence: " + lastReadStreamSeq);
                        reportAt = System.currentTimeMillis() + CONSUME_REPORT_FREQUENCY;
                    }
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static class ResilientPublisher implements Runnable {
        JetStream js;
        String subject;
        String prefix;
        int count;
        long delay;

        public ResilientPublisher(JetStream js, String subject, String prefix, int count, long delay) {
            this.js = js;
            this.subject = subject;
            this.prefix = prefix;
            this.count = count;
            this.delay = delay;
        }

        public void run() {
            boolean lastOk = false;
            long reportAt = 0;
            long count = 0;
            while (true) {
                try {
                    if (delay > 0) {
                        Thread.sleep(delay);
                    }
                    String data = prefix + "-" + (++count);
                    Message msg = NatsMessage.builder()
                        .subject(subject)
                        .data(data.getBytes(StandardCharsets.US_ASCII))
                        .build();
                    PublishAck pa = js.publish(msg);
                    if (!lastOk) {
                        reportAt = 0; // so it reports this time
                    }
                    lastOk = true;
                    if (System.currentTimeMillis() > reportAt) {
                        report("Published Sequence: " + pa.getSeqno());
                        reportAt = System.currentTimeMillis() + PRINT_REPORT_FREQUENCY;
                    }
                }
                catch (Exception e) {
                    if (lastOk || System.currentTimeMillis() > reportAt) {
                        report("Publish Exception: " + e);
                        reportAt = System.currentTimeMillis() + PRINT_REPORT_FREQUENCY;
                    }
                    lastOk = false;
                    try {
                        Thread.sleep(PUBLISH_DELAY);
                    }
                    catch (InterruptedException ignore) {
                    }
                }
            }
        };
    }

    private static Thread getMiscReportingThread(JetStreamManagement jsm) {
        return new Thread(() -> {
            while (true) {
                try {
                    StreamInfo si = jsm.getStreamInfo(STREAM);
                    report("Stream Configuration:" + si.getConfiguration().toJson());
                    report(si.getClusterInfo().toString());
                    report(si.getStreamState().toString());
                    Thread.sleep(MISC_REPORT_FREQUENCY);
                }
                catch (Exception e) {
                    report("Misc Reporting Exception: " + e);
                }
            }
        });
    }

    public static void setupStream(JetStreamManagement jsm, String stream, String subject) {
        // in case the stream was here before, we want a completely new one
        try { jsm.deleteStream(stream); } catch (Exception ignore) {}

        try {
            jsm.addStream(StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.File)
                .subjects(subject)
                .build());
        }
        catch (Exception e) {
            System.err.println("Fatal error, cannot create stream.");
            System.exit(-1);
        }
    }

    public static void report(String message) {
        String t = "" + System.currentTimeMillis();
        System.out.println("[" + t.substring(t.length() - 9) + "] " + message);
    }
}
