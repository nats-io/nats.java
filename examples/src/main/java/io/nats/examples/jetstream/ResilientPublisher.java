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
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.impl.ErrorListenerConsoleImpl;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

/**
 * This example will demonstrate a resilient publish
 * HOW TO TEST
 * 1. Set up and run a simple cluster. See https://github.com/nats-io/java-nats-examples/tree/main/example-cluster-config
 * 2. Run this program, watch the output for some time
 * 3. Kill the server that is the leader, let it stay down for a short time or a long time
 * 4. See the output showing things aren't running.
 * 5. Bring the killed server back up and watch the output showing recovery.
 */
public class ResilientPublisher implements Runnable {
    public static void main(String[] args) {
        Options options = Options.builder()
            .socketWriteTimeout(20_000)
            .connectionListener((conn, type) -> System.out.println(type))
            .errorListener(new ErrorListenerConsoleImpl())
            .build();
        try (Connection nc = Nats.connect(options)) {

// JetStream PUBLISHER EXAMPLE
            JetStreamManagement jsm = nc.jetStreamManagement();
            createOrReplaceStream(jsm, "js-stream", StorageType.Memory, "js-subject");
            ResilientPublisher rp = new ResilientPublisher(nc, jsm, "js-stream", "js-subject")
                .basicDataPrefix("data")
                .delay(1)
                .reportFrequency(1000);
// END JetStream PUBLISHER EXAMPLE

// CORE PUBLISHER EXAMPLE
//            ResilientPublisher rp = new ResilientPublisher(nc, "core-subject")
//                .basicDataPrefix("data")
//                .delay(1)
//                .reportFrequency(1000);
// END CORE PUBLISHER EXAMPLE

            Thread t = new Thread(rp);
            t.start();
            t.join();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    enum ReportStyle {
        Time, Labeled, Plain
    }

    private final Connection nc;
    private final JetStreamManagement jsm;
    private final JetStream js;
    private final String stream;
    private final String subject;
    private final AtomicLong lastPub;
    private final AtomicBoolean keepGoing;

    private boolean expectationCheck;
    private long jitter;
    private long delay;
    private boolean reporting;
    private long reportFrequency;
    private ReportStyle reportStyle;
    private Function<Long, byte[]> dataProvider;
    private java.util.function.BiConsumer<Connection, Long> beforePublish;
    private java.util.function.BiConsumer<Connection, PublishAck> afterPublish;
    private java.util.function.BiConsumer<Connection, Long> publishReporter;
    private java.util.function.BiConsumer<Connection, Exception> exceptionReporter;

    public ResilientPublisher(Connection nc, String subject) {
        this(nc, null, null, subject);
    }

    public ResilientPublisher(Connection nc, JetStreamManagement jsm, String stream, String subject) {
        this.nc = nc;
        if (jsm == null) {
            this.jsm = null;
            js = null;
            this.stream = null;
        }
        else {
            this.jsm = jsm;
            js = jsm.jetStream();
            this.stream = stream;
        }
        this.subject = subject;
        lastPub = new AtomicLong();
        keepGoing = new AtomicBoolean(true);
        reportStyle = ReportStyle.Plain;
        basicDataPrefix(null);
        beforePublish(null);
        afterPublish(null);
        publishReporter(null);
        exceptionReporter(null);
    }

    public ResilientPublisher expectationCheck(boolean expectationCheck) {
        this.expectationCheck = expectationCheck;
        return this;
    }

    public ResilientPublisher jitter(long jitter) {
        this.jitter = jitter;
        return this;
    }

    public ResilientPublisher delay(long delay) {
        this.delay = delay;
        return this;
    }

    public ResilientPublisher reportStyle(ReportStyle reportStyle) {
        this.reportStyle = reportStyle;
        return this;
    }

    public ResilientPublisher reportFrequency(long reportFrequency) {
        this.reportFrequency = reportFrequency;
        reporting = reportFrequency > 0;
        return this;
    }

    public ResilientPublisher basicDataPrefix(String prefix) {
        dataProvider = prefix == null ? l -> null : l -> (prefix + "-" + l).getBytes();
        return this;
    }

    public ResilientPublisher dataProvider(Function<Long, byte[]> dataProvider) {
        this.dataProvider = dataProvider == null ? l -> null : dataProvider;
        return this;
    }

    public ResilientPublisher beforePublish(BiConsumer<Connection, Long> beforePublish) {
        this.beforePublish = beforePublish == null ? (c, l) -> {} : beforePublish;
        return this;
    }

    boolean lastPublishOk = false;

    public ResilientPublisher afterPublish(BiConsumer<Connection, PublishAck> afterPublish) {
        this.afterPublish = afterPublish == null
            ? (c, l) -> { if (!lastPublishOk) { report("Publish Start/Resume: " + l); lastPublishOk = true; } }
            : afterPublish;
        return this;
    }

    public ResilientPublisher publishReporter(BiConsumer<Connection, Long> publishReporter) {
        this.publishReporter = publishReporter == null
            ? (c, l) -> report("Published Id: " + l)
            : publishReporter;
        return this;
    }

    public ResilientPublisher exceptionReporter(BiConsumer<Connection, Exception> exceptionReporter) {
        this.exceptionReporter = exceptionReporter == null
            ? (c, e) -> { if (lastPublishOk) { report("Publish Exception: " + e); lastPublishOk = false; }}
            : exceptionReporter;
        return this;
    }

    private void report(String message) {
        if (reportStyle == ReportStyle.Time) {
            NatsJsUtils.report(message);
        }
        else if (reportStyle == ReportStyle.Labeled) {
            System.out.println("[Resilient Publisher] " + message);
        }
        else {
            System.out.println(message);
        }
    }

    public void stop() {
        keepGoing.set(false);
    }

    public long getLastPub() {
        return lastPub.get();
    }

    @Override
    public void run() {
        Exception lastEx = null;
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
                long lastPubId = lastPub.get();
                long pubId = lastPub.incrementAndGet();

                beforePublish.accept(nc, pubId);
                if (js == null) {
                    nc.publish(subject, dataProvider.apply(pubId));
                }
                else {
                    PublishOptions po = expectationCheck
                        ? PublishOptions.builder().expectedLastSequence(lastPubId).build()
                        : null;
                    PublishAck pa = js.publish(subject, dataProvider.apply(pubId), po);
                    afterPublish.accept(nc, pa);
                }

                if (reporting) {
                    if (lastEx != null || System.currentTimeMillis() > reportAt) {
                        publishReporter.accept(nc, pubId);
                        reportAt = System.currentTimeMillis() + reportFrequency;
                    }
                }

                lastEx = null;
            }
            catch (Exception e) {
                boolean diff = lastEx == null;
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
                    if (!diff && lastEx instanceof JetStreamApiException) {
                        diff = j.getApiErrorCode() != ((JetStreamApiException)lastEx).getApiErrorCode();
                    }
                }
                if (!diff && lastEx.getClass().getSimpleName().equals(e.getClass().getSimpleName())){
                    diff = true;
                }
                if (diff || System.currentTimeMillis() > reportAt) {
                    exceptionReporter.accept(nc, e);
                    reportAt = System.currentTimeMillis() + reportFrequency;
                }
                lastEx = e;
            }
        }
    }
}