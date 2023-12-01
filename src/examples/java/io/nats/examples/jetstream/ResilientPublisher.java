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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StorageType;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;
import static io.nats.examples.jetstream.NatsJsUtils.report;

/**
 * This example will demonstrate simplified fetch that is resilient
 * HOW TO TEST
 * 1. Set up and run a simple cluster. See https://github.com/nats-io/java-nats-examples/tree/main/example-cluster-config
 * 2. Run this program, watch the output for some time
 * 3. Kill the server that is the leader, let it stay down for a short time or a long time
 * 4. See the output showing things aren't running.
 * 5. Bring the killed server back up and watch the output showing recovery.
 */
public class ResilientPublisher implements Runnable {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createOrReplaceStream(jsm, "stream", StorageType.Memory, "subject");
            ResilientPublisher rp = newInstanceReportingAndDelay(jsm, "stream", "subject", "prefix", 100, 1000);
            Thread t = new Thread(rp);
            t.start();
            t.join();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ResilientPublisher newInstanceReportingAndDelay(JetStreamManagement jsm, String stream, String subject, String prefix, long delay, long reportFrequency) {
        return new ResilientPublisher(jsm, stream, subject, prefix, -1, delay, reportFrequency);
    }

    public static ResilientPublisher newInstanceReportingAndJitter(JetStreamManagement jsm, String stream, String subject, String prefix, long jitter, long reportFrequency) {
        return new ResilientPublisher(jsm, stream, subject, prefix, jitter, -1, reportFrequency);
    }

    public static ResilientPublisher newInstanceQuietAndDelay(JetStreamManagement jsm, String stream, String subject, String prefix, long delay) {
        return new ResilientPublisher(jsm, stream, subject, prefix, -1, delay, null);
    }

    public static ResilientPublisher newInstanceQuietAndJitter(JetStreamManagement jsm, String stream, String subject, String prefix, long jitter) {
        return new ResilientPublisher(jsm, stream, subject, prefix, jitter, -1, null);
    }

    private final JetStreamManagement jsm;
    private final JetStream js;
    private final String stream;
    private final String subject;
    private final String prefix;
    private final long jitter;
    private final long delay;
    private final boolean reporting;
    private final Long reportFrequency;
    private final AtomicBoolean keepGoing = new AtomicBoolean(true);
    private final AtomicLong lastPub;
    private final Function<Long, byte[]> dataProvider;

    public ResilientPublisher(JetStreamManagement jsm, String stream, String subject, String prefix, long jitter, long delay, Long reportFrequency) {
        this.jsm = jsm;
        js = jsm.jetStream();
        this.stream = stream;
        this.subject = subject;
        this.prefix = prefix;
        this.jitter = jitter;
        this.delay = delay;
        this.reporting = reportFrequency != null;
        this.reportFrequency = reportFrequency;
        lastPub = new AtomicLong();

        dataProvider = prefix == null ? l -> null : l -> (prefix + "-" + l).getBytes();
    }

    public void stop() {
        keepGoing.set(false);
    }

    public long getLastPub() {
        return lastPub.get();
    }

    @Override
    public void run() {
        boolean lastWasOk = false;
        long reportAt = 0;
        while (keepGoing.get()) {
            try {
                if (jitter > 0) {
                    //noinspection BusyWait
                    Thread.sleep(ThreadLocalRandom.current().nextLong(jitter));
                }
                if (delay > 0) {
                    //noinspection BusyWait
                    Thread.sleep(delay);
                }

                // it's possible that the publish was not recorded
                // but we won't find out until the next round
                // at which time we will get the 10071
                PublishOptions po = PublishOptions.builder()
                    .expectedLastSequence(lastPub.get())
                    .build();
                js.publish(subject, dataProvider.apply(lastPub.incrementAndGet()), po);

                if (reporting) {
                    if (!lastWasOk || System.currentTimeMillis() > reportAt) {
                        report("Published Sequence: " + lastPub.get());
                        reportAt = System.currentTimeMillis() + reportFrequency;
                    }
                }

                lastWasOk = true;
            }
            catch (Exception e) {
                if (e instanceof JetStreamApiException) {
                    JetStreamApiException j = (JetStreamApiException)e;
                    if (j.getApiErrorCode() == 10071) {
                        try {
                            MessageInfo mi = jsm.getLastMessage(stream, subject);
                            lastPub.set(mi.getSeq());
                        }
                        catch (Exception ignore) {
                            // ignore, it will happen again!
                        }
                    }
                }
                if (lastWasOk || System.currentTimeMillis() > reportAt) {
                    report("Publish Exception: " + e);
                    reportAt = System.currentTimeMillis() + reportFrequency;
                }
                lastWasOk = false;
            }
        }
    }
}