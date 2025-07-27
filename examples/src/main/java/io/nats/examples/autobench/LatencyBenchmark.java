// Copyright 2015-2018 The NATS Authors
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

package io.nats.examples.autobench;

import io.nats.client.*;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LatencyBenchmark extends AutoBenchmark {
    static FileOutputStream lcsvOut;
    static List<Long> allPayloadSizes;
    static List<List<Long>> allMeasurements;

    // We only touch this in subThread, until test is done so no locking
    final ArrayList<Long> measurements = new ArrayList<>((int)this.getMessageCount());
    final String lcsv;

    public LatencyBenchmark(String name, long messageCount, long messageSize, String lcsv) {
        super(name, messageCount, messageSize);
        this.lcsv = lcsv;
    }

    public void execute(Options connectOptions) throws InterruptedException {
        byte[] payload = createPayload();
        String subject = getSubject();

        final CompletableFuture<Void> go = new CompletableFuture<>();
        final CompletableFuture<Void> subReady = new CompletableFuture<>();
        final CompletableFuture<Void> pubReady = new CompletableFuture<>();
        final CompletableFuture<Void> subDone = new CompletableFuture<>();
        final CompletableFuture<Void> pubDone = new CompletableFuture<>();
        final CyclicBarrier lockStep = new CyclicBarrier(2);

        final AtomicLong start = new AtomicLong();

        Thread subThread = new Thread(() -> {
            try {
                Connection subConnect = Nats.connect(connectOptions);
                if (subConnect.getStatus() != Connection.Status.CONNECTED) {
                    throw new Exception("Unable to connect");
                }
                try {
                    Subscription sub = subConnect.subscribe(subject);
                    subConnect.flush(Duration.ofSeconds(5));
                    subReady.complete(null);
                    go.get();
                    
                    int count = 0;
                    while(count < this.getMessageCount()) {
                        Message msg = sub.nextMessage(Duration.ofSeconds(5));

                        if (msg != null){
                            measurements.add(System.nanoTime() - start.get());
                            count++;
                            lockStep.await(5000, TimeUnit.MILLISECONDS);
                        }
                    }

                    subDone.complete(null);
                } catch (Exception exp) {
                    this.setException(exp);
                } finally {
                    subConnect.close();
                }
            } catch (Exception ex) {
                subReady.cancel(true);
                this.setException(ex);
            } finally {
                subDone.complete(null);
            }
        }, "Latency Test - Subscriber");
        subThread.start();

        Thread pubThread = new Thread(() -> {
            try {
                Connection pubConnect = Nats.connect(connectOptions);
                if (pubConnect.getStatus() != Connection.Status.CONNECTED) {
                    throw new Exception("Unable to connect");
                }
                try {
                    pubReady.complete(null);
                    go.get();
                    
                    for(int i = 0; i < this.getMessageCount(); i++) {
                        lockStep.reset();
                        start.set(System.nanoTime());
                        pubConnect.publish(subject, payload);
                        try {pubConnect.flush(Duration.ofMillis(5000));}catch(Exception e){}
                        lockStep.await();
                    }
                    
                    pubDone.complete(null);
                } finally {
                    pubConnect.close();
                }
            } catch (Exception ex) {
                pubReady.cancel(true);
                this.setException(ex);
            } finally {
                pubDone.complete(null);
            }
        }, "Latency Test - Publisher");
        pubThread.start();

        getFutureSafely(subReady);
        getFutureSafely(pubReady);

        if (this.getException() != null) {
            go.complete(null); // just in case the other thread is waiting
            return;
        }
        
        go.complete(null);
        getFutureSafely(pubDone);
        getFutureSafely(subDone);
    }

    @Override
    public void beforePrintFirstOfKind() {
        System.out.println("Latency                 |           nanos              |            |");
        System.out.println("| payload     |   count |    min |  median  |      max | std dev ms |");
        System.out.println("| ----------- | ------- | ------ | -------- | -------- | ---------- |");

        if (lcsv != null) {
            try {
                lcsvOut = new FileOutputStream(lcsv);
                allPayloadSizes = new ArrayList<>();
                allMeasurements = new ArrayList<>();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void printResult() {
        if (this.getException() != null) {
            String message = this.getException().getMessage();

            if (message == null) {
                message = this.getException().getClass().getCanonicalName();
            }

            System.out.printf("%-18s Exception: %12s\n", getName(), message);
            return;
        }

        if (allMeasurements != null) {
            allPayloadSizes.add(getMessageSize());
            allMeasurements.add(measurements);
        }

        LongSummaryStatistics stats = measurements.stream().
                                mapToLong(Long::longValue).
                                collect(LongSummaryStatistics::new,
                                        LongSummaryStatistics::accept,
                                        LongSummaryStatistics::combine);
        // Convert to micro-seconds
        long min = stats.getMin() / 1_000;
        long max = stats.getMax() / 1_000;
        long count = stats.getCount();
        double average = stats.getAverage() / 1e3;
        double median = calcMedian() / 1e3;
        double stdDev = Math.sqrt(measurements.stream().
                                            mapToDouble(Long::doubleValue).
                                            map(d -> ((d-average) * (d-average))).
                                            sum()) / (1e3 * (count-1));

        System.out.printf("| %-11s | %7s | %6s | %8.2f | %8s | +/- %6.2f |\n",
                            getName().replaceAll("Latency ", "") + " bytes",
                            NumberFormat.getIntegerInstance().format(count),
                            NumberFormat.getIntegerInstance().format(min),
                            median,
                            NumberFormat.getIntegerInstance().format(max),
                            stdDev);
    }

    @Override
    public void afterPrintLastOfKind() {
        if (allMeasurements != null) {
            try {
                int cols = allPayloadSizes.size();
                int rows = allMeasurements.get(0).size();
                StringBuilder sb = new StringBuilder();
                for (int c = 0; c < cols; c++) {
                    if (c > 0) {
                        sb.append(",");
                    }
                    sb.append(allPayloadSizes.get(c)).append(" bytes");
                }
                sb.append("\r\n");
                lcsvOut.write(sb.toString().getBytes(StandardCharsets.US_ASCII));

                for (int r = 0; r < rows; r++) {
                    sb.setLength(0);
                    for (int c = 0; c < cols; c++) {
                        if (c > 0) {
                            sb.append(",");
                        }
                        sb.append(allMeasurements.get(c).get(r));
                    }
                    sb.append("\r\n");
                    lcsvOut.write(sb.toString().getBytes(StandardCharsets.US_ASCII));
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public double calcMedian() {

        int size = measurements.size();
        int middle = measurements.size() / 2;

        measurements.sort(Long::compareTo);

        if (size % 2 == 1) {
            return measurements.get(middle).longValue();
        } else {
            double low = measurements.get(middle-1).doubleValue() / 2.0;
            double high = measurements.get(middle).doubleValue() / 2.0;
            return high + low; // already divided by 2
        }
    }
}