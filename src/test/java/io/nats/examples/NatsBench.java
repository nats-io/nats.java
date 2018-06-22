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

package io.nats.examples;

import io.nats.benchmark.Benchmark;
import io.nats.benchmark.Sample;
import io.nats.client.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A utility class for measuring NATS performance.
 */
public class NatsBench {
    final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<Throwable>();

    // Default test values
    private int numMsgs = 100000;
    private int numPubs = 1;
    private int numSubs = 0;
    private int size = 128;

    private String urls = Options.DEFAULT_URL;
    private String subject;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong replies = new AtomicLong();
    private boolean csv = false;

    private Thread shutdownHook;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private boolean secure = false;
    private Benchmark bench;

    static final String usageString =
            "\nUsage: java NatsBench [-s server] [-tls] [-np num] [-ns num] [-n num] [-ms size] "
                    + "[-csv file] <subject>\n\nOptions:\n"
                    + "    -s   <urls>                    The nats server URLs (comma-separated)\n"
                    + "    -tls                            Use TLS secure connection (false)\n"
                    + "    -np                             Number of concurrent publishers (1)\n"
                    + "    -ns                             Number of concurrent subscribers (0)\n"
                    + "    -n                              Number of messages to publish (100,000)\n"
                    + "    -ms                             Size of the message (128)\n"
                    + "    -csv                            Print results to stdout as csv (false)\n";

    /**
     * Main constructor for NatsBench.
     *
     * @param args configuration parameters
     */
    public NatsBench(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            usage();
            return;
        }
        parseArgs(args);
    }

    /**
     * Properties-based constructor for NatsBench.
     *
     * @param properties configuration properties
     */
    public NatsBench(Properties properties) throws NoSuchAlgorithmException {
        urls = properties.getProperty("bench.nats.servers", urls);
        secure = Boolean.parseBoolean(
                properties.getProperty("bench.nats.secure", Boolean.toString(secure)));
        numMsgs = Integer.parseInt(
                properties.getProperty("bench.nats.msg.count", Integer.toString(numMsgs)));
        size = Integer
                .parseInt(properties.getProperty("bench.nats.msg.size", Integer.toString(numSubs)));
        numPubs = Integer
                .parseInt(properties.getProperty("bench.nats.pubs", Integer.toString(numPubs)));
        numSubs = Integer
                .parseInt(properties.getProperty("bench.nats.subs", Integer.toString(numSubs)));
        csv = Boolean.parseBoolean(
            properties.getProperty("bench.nats.csv", Boolean.toString(csv)));
        subject = properties.getProperty("bench.nats.subject", NUID.nextGlobal());
    }

    Options prepareOptions(boolean secure) throws NoSuchAlgorithmException {
        String[] servers = urls.split(",");
        Options.Builder builder = new Options.Builder();
        builder.noReconnect();
        builder.servers(servers);

        if (secure) {
            builder.secure();
        }

        return builder.build();
    }

    class Worker implements Runnable {
        final Future<Boolean> starter;
        final Phaser finisher;
        final int numMsgs;
        final int size;
        final boolean secure;

        Worker(Future<Boolean> starter, Phaser finisher, int numMsgs, int size, boolean secure) {
            this.starter = starter;
            this.finisher = finisher;
            this.numMsgs = numMsgs;
            this.size = size;
            this.secure = secure;
        }

        @Override
        public void run() {
        }
    }

    class SyncSubWorker extends Worker {
        SyncSubWorker(Future<Boolean> starter, Phaser finisher, int numMsgs, int size, boolean secure) {
            super(starter, finisher, numMsgs, size, secure);
        }

        @Override
        public void run() {
            try {
                Options opts = prepareOptions(this.secure);
                Connection nc = Nats.connect(opts);

                Subscription sub = nc.subscribe(subject);
                nc.flush(null);

                // Signal we are ready
                finisher.arrive();

                // Wait for the signal to start tracking time
                starter.get(5000, TimeUnit.MICROSECONDS);

                Duration timeout = Duration.ofMillis(500);

                final long start = System.nanoTime();

                for (int i=0;i<numMsgs;i++) {
                    sub.nextMessage(timeout);
                    received.incrementAndGet();
                }

                bench.addSubSample(new Sample(numMsgs, size, start, System.nanoTime(), nc.getStatistics()));

                // Clean up
                sub.unsubscribe();
                nc.close();
            } catch (Exception e) {
                errorQueue.add(e);
            } finally {
                finisher.arrive();
            }
        }
    }

    class PubWorker extends Worker {
        PubWorker(Future<Boolean> starter, Phaser finisher, int numMsgs, int size, boolean secure) {
            super(starter, finisher, numMsgs, size, secure);
        }

        @Override
        public void run() {
            try {
                Options opts = prepareOptions(this.secure);
                Connection nc = Nats.connect(opts);

                byte[] payload = null;
                if (size > 0) {
                    payload = new byte[size];
                }

                 // Wait for the signal
                starter.get(5000, TimeUnit.MICROSECONDS);
                long start = System.nanoTime();
                for (int i = 0; i < numMsgs; i++) {
                    sent.incrementAndGet();
                    nc.publish(subject, payload);
                }
                nc.flush(Duration.ZERO);
                long end = System.nanoTime();

                bench.addPubSample(new Sample(numMsgs, size, start, end, nc.getStatistics()));
                nc.close();
            } catch (Exception e) {
                errorQueue.add(e);
            } finally {
                finisher.arrive();
            }
        }
    }

    /**
     * Runs the benchmark.
     *
     * @throws Exception if an exception occurs
     */
    public void start() throws Exception {
        installShutdownHook();

        System.out.println();
        System.out.printf("Starting benchmark(s) [msgs=%d, msgsize=%d, pubs=%d, subs=%d]\n", numMsgs,
                size, numPubs, numSubs);
        System.out.println("Each pub and sub uses a separate thread and connection to the gnatsd.");
        System.out.println("Use ctrl-C to cancel.");
        System.out.println();

        //runTest("Pub Only", this.numPubs, 0);
        runTest("Pub/Sub", this.numPubs, this.numSubs);

        Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }

    public void runTest(String title, int pubCount, int subCount) throws Exception {
        final Phaser finisher = new Phaser();
        final CompletableFuture<Boolean> starter = new CompletableFuture<>();
        finisher.register();
        sent.set(0);
        received.set(0);

        bench = new Benchmark(title);

        // Run Subscribers first
        for (int i = 0; i < subCount; i++) {
            finisher.register();
            new Thread(new SyncSubWorker(starter, finisher, this.numMsgs, this.size, secure)).start();
        }

        // Wait for subscribers threads to initialize
        finisher.arriveAndAwaitAdvance();

        if (!errorQueue.isEmpty()) {
            Throwable error = errorQueue.take();
            System.err.printf(error.getMessage());
            error.printStackTrace();
            throw new RuntimeException(error);
        }

        // Now publishers
        int remaining = this.numMsgs;
        int perPubMsgs = this.numMsgs / pubCount;
        for (int i = 0; i < pubCount; i++) {
            finisher.register();
            if (i == numPubs - 1) {
                perPubMsgs = remaining;
            }
            
            new Thread(new PubWorker(starter, finisher, perPubMsgs, this.size, secure)).start();
            
            remaining -= perPubMsgs;
        }

        // Start everything running
        starter.complete(Boolean.TRUE);

        // Wait for subscribers and publishers to finish
        finisher.arriveAndAwaitAdvance();

        if (!errorQueue.isEmpty()) {
            Throwable error = errorQueue.take();
            System.err.printf("Error running test [%s]\n", error.getMessage());
            System.err.printf("Latest test sent = %d\n", sent.get());
            System.err.printf("Latest test received = %d\n", received.get());
            System.err.printf("Latest test repies = %d\n", replies.get());
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
            throw new RuntimeException(error);
        }

        bench.close();

        if (csv) {
            System.out.println(bench.csv());
        } else {
            System.out.println(bench.report());
        }
    }

    void installShutdownHook() {
        shutdownHook = new Thread(new Runnable() {
            @Override
            public void run() {
                System.err.println("\nCaught CTRL-C, shutting down gracefully...\n");
                shutdown.set(true);
                System.err.printf("Sent=%d\n", sent.get());
                System.err.printf("Received=%d\n", received.get());

            }
        });

        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }

    private void parseArgs(String[] args) {
        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        subject = argList.get(argList.size() - 1);
        argList.remove(argList.size() - 1);

        // Anything left is flags + args
        Iterator<String> it = argList.iterator();
        while (it.hasNext()) {
            String arg = it.next();
            switch (arg) {
                case "-s":
                case "--server":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    urls = it.next();
                    it.remove();
                    continue;
                case "-tls":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    secure = true;
                    continue;
                case "-np":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    numPubs = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-ns":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    numSubs = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-n":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    numMsgs = Integer.parseInt(it.next());
                    // hashModulo = numMsgs / 100;
                    it.remove();
                    continue;
                case "-ms":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    size = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-csv":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    csv = true;
                    continue;
                default:
                    System.err.printf("Unexpected token: '%s'\n", arg);
                    usage();
                    break;
            }
        }
    }

    private static Properties loadProperties(String configPath) {
        try {
            InputStream is = new FileInputStream(configPath);
            Properties prop = new Properties();
            prop.load(is);
            return prop;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The main program executive.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {

        try {
            System.out.println("HURRY ...");
            Thread.sleep(10*1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Properties properties = null;
        try {
            if (args.length == 1 && args[0].endsWith(".properties")) {
                properties = loadProperties(args[0]);
                new NatsBench(properties).start();
            } else {
                new NatsBench(args).start();
            }
        } catch (Exception e) {
            System.err.printf("Exiting due to exception [%s]\n", e.getMessage());
            System.exit(-1);
        }
        try {
            System.out.println("Take your time ...");
            Thread.sleep(20*60*60*1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

}
