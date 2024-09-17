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
import io.nats.client.api.StorageType;
import io.nats.examples.jetstream.ResilientPublisher;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.nats.examples.jetstream.NatsJsUtils.*;

/**
 * This example will demonstrate simplified fetch that is resilient
 * HOW TO TEST
 * 1. Set up and run a simple cluster. See <a href="https://github.com/nats-io/java-nats-examples/tree/main/example-cluster-config">https://github.com/nats-io/java-nats-examples/tree/main/example-cluster-config</a>
 * 2. Run this program, watch the output for some time
 * 3. Kill the server that is the leader, let it stay down for a short time or a long time
 * 4. See the output showing things aren't running.
 * 5. Bring the killed server back up and watch the output showing recovery.
 */
public class FetchResilientExample implements Runnable {
    static final String STREAM = "fetch-resilient-stream";
    static final String SUBJECT = "fetch-resilient-subject";
    static final String CONSUMER_NAME = "fetch-resilient-consumer";
    static final String MESSAGE_PREFIX = "fetch-resilient-message";
    static final int FETCH_SIZE = 500;
    static final int PUBLISH_DELAY = 100;
    static final long CONSUME_REPORT_FREQUENCY = 2000;
    static final long PUB_REPORT_FREQUENCY = 5000;
    static final long STREAM_REPORT_FREQUENCY = 30000;

    static String SERVER = "nats://localhost:4222";
    public final AtomicBoolean keepGoing = new AtomicBoolean(true);
    private final ConsumerContext cc;
    private long reportAt = 0;
    public FetchResilientExample(ConsumerContext cc) {
        this.cc = cc;
    }

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER)
                .connectionListener((c, t) -> {
                    report("Connection: " + c.getServerInfo().getPort() + " " + t);
                })
                .build();

        try (Connection nc = Nats.connect(options)) {
            final JetStreamManagement jsm = nc.jetStreamManagement();

            createOrReplaceStream(jsm, STREAM, StorageType.File, SUBJECT);

            Thread streamReportingThread = getStreamReportingThread(jsm, STREAM, STREAM_REPORT_FREQUENCY);
            streamReportingThread.start();

            // simulating a publisher somewhere else.
            ResilientPublisher rp = new ResilientPublisher(nc, jsm, STREAM, SUBJECT)
                    .basicDataPrefix(MESSAGE_PREFIX)
                    .delay(PUBLISH_DELAY)
                    .reportFrequency(PUB_REPORT_FREQUENCY);
            Thread pubThread = new Thread(rp);
            pubThread.start();

            JetStream js = jsm.jetStream();

            StreamContext sc = js.getStreamContext(STREAM);
            ConsumerContext cc = sc.createOrUpdateConsumer(
                    ConsumerConfiguration.builder()
                            .durable(CONSUMER_NAME)
                            .filterSubject(SUBJECT)
                            .build());

            FetchResilientExample example = new FetchResilientExample(cc);
            Thread consumeThread = new Thread(example);
            consumeThread.start();

            consumeThread.join(); // just letting this run
        } catch (IOException ioe) {
            // problem making the connection or
        } catch (InterruptedException e) {
            // thread interruption in the body of the example
        } catch (JetStreamApiException e) {
            // some api exception
        }
    }

    public void stop() {
        keepGoing.set(false);
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
            } catch (Exception e) {
                // do we care if the autocloseable FetchConsumer errors on close?
                // probably not, but maybe log it.
            }

            // simulating some work to be done between fetches
            try {
                if (System.currentTimeMillis() > reportAt) {
                    report("Last Read Sequence: " + lastReadStreamSeq);
                    reportAt = System.currentTimeMillis() + CONSUME_REPORT_FREQUENCY;
                }
                //noinspection BusyWait
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
