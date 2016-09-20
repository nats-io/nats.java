/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.examples;

import io.nats.benchmark.Benchmark;
import io.nats.benchmark.Sample;
import io.nats.client.AsyncSubscription;
import io.nats.client.ClosedCallback;
import io.nats.client.Connection;
import io.nats.client.ConnectionEvent;
import io.nats.client.ConnectionFactory;
import io.nats.client.DisconnectedCallback;
import io.nats.client.ExceptionHandler;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NATSException;
import io.nats.client.Subscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A utility class for measuring NATS performance.
 *
 */
public class NatsBench {
    static final Logger log = LoggerFactory.getLogger(NatsBench.class);

    // Default test values
    private int numMsgs = 100000;
    private int numPubs = 1;
    private int numSubs = 0;
    private int size = 128;

    private String urls = ConnectionFactory.DEFAULT_URL;
    private String subject;
    private AtomicInteger sent = new AtomicInteger();
    private AtomicInteger received = new AtomicInteger();
    private String csvFileName;

    private ConnectionFactory cf;
    private Thread shutdownHook;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private boolean secure;
    private Benchmark bench;

    static final String usageString =
            "\nUsage: java NatsBench [-s server] [-tls] [-np num] [-ns num] [-n num] [-ms size] "
                    + "[-csv file] <subject>\n\nOptions:\n"
                    + "    -s   <urls>                    The nats server URLs (comma-separated)\n"
                    + "    -tls                            Use TLS secure connection\n"
                    + "    -np                             Number of concurrent publishers\n"
                    + "    -ns                             Number of concurrent subscribers\n"
                    + "    -n                              Number of messages to publish\n"
                    + "    -ms                             Size of the message\n"
                    + "    -csv                            Save bench data to csv file\n";

    /**
     * Main constructor for NatsBench.
     * 
     * @param args configuration parameters
     */
    public NatsBench(String[] args) {
        if (args == null || args.length < 1) {
            usage();
            return;
        }
        parseArgs(args);
        cf = new ConnectionFactory(urls);
        cf.setSecure(secure);
        cf.setReconnectAllowed(false);

        bench = new Benchmark("NATS", numSubs, numPubs);
    }

    class Worker implements Runnable {
        // protected final CountDownLatch startLatch;
        // protected final CountDownLatch doneLatch;
        protected final Phaser phaser;
        protected final int num;
        protected final int size;

        Worker(Phaser phaser, int numMsgs, int size) {
            this.phaser = phaser;
            this.num = numMsgs;
            this.size = size;
        }

        @Override
        public void run() {}
    }

    class SubWorker extends Worker {
        SubWorker(Phaser phaser, int numMsgs, int size) {
            super(phaser, numMsgs, size);
        }

        @Override
        public void run() {
            try {
                runSubscriber();
            } catch (Exception e) {
                e.printStackTrace();
                phaser.arrive();
            }
        }

        public void runSubscriber() throws Exception {
            final Connection nc = cf.createConnection();
            nc.setDisconnectedCallback(new DisconnectedCallback() {
                @Override
                public void onDisconnect(ConnectionEvent ev) {
                    log.error("Subscriber disconnected after {} msgs", received.get());
                }
            });
            nc.setClosedCallback(new ClosedCallback() {
                @Override
                public void onClose(ConnectionEvent ev) {
                    log.error("Subscriber connection closed after {} msgs", received.get());
                }
            });
            nc.setExceptionHandler(new ExceptionHandler() {
                @Override
                public void onException(NATSException ex) {
                    log.error("Subscriber connection exception", ex);
                    AsyncSubscription sub = (AsyncSubscription) ex.getSubscription();
                    log.error("Sent={}, Received={}", sent.get(), received.get());
                    log.error("Messages dropped (total) = {}", sub.getDropped());
                    System.exit(-1);
                }
            });

            final long start = System.nanoTime();
            Subscription sub = nc.subscribe(subject, new MessageHandler() {
                @Override
                public void onMessage(Message msg) {
                    received.incrementAndGet();
                    if (received.get() >= numMsgs) {
                        bench.addSubSample(new Sample(numMsgs, size, start, System.nanoTime(), nc));
                        phaser.arrive();
                        nc.setDisconnectedCallback(null);
                        nc.setClosedCallback(null);
                        nc.close();
                    }
                }
            });
            sub.setPendingLimits(10000000, 1000000000);
            nc.flush();
            phaser.arrive();
            while (received.get() < numMsgs) {
            }
        }
    }


    class PubWorker extends Worker {
        PubWorker(Phaser phaser, int numMsgs, int size) {
            super(phaser, numMsgs, size);
        }

        @Override
        public void run() {
            try {
                runPublisher();
                phaser.arrive();
            } catch (Exception e) {
                e.printStackTrace();
                phaser.arrive();
            }
        }

        public void runPublisher() throws Exception {
            try (Connection nc = cf.createConnection()) {
                byte[] payload = null;
                if (size > 0) {
                    payload = new byte[size];
                }

                final long start = System.nanoTime();

                for (int i = 0; i < numMsgs; i++) {
                    sent.incrementAndGet();
                    nc.publish(subject, payload);
                }
                nc.flush();
                bench.addPubSample(new Sample(numMsgs, size, start, System.nanoTime(), nc));
            }
        }
    }

    /**
     * Runs the benchmark.
     * 
     * @throws Exception if an exception occurs
     */
    public void run() throws Exception {
        ExecutorService exec = Executors.newCachedThreadPool();
        final Phaser phaser = new Phaser();

        installShutdownHook();

        phaser.register();

        // Run Subscribers first
        for (int i = 0; i < numSubs; i++) {
            phaser.register();
            exec.execute(new SubWorker(phaser, numMsgs, size));
        }

        // Wait for subscribers threads to initialize
        phaser.arriveAndAwaitAdvance();

        // Now publishers
        for (int i = 0; i < numPubs; i++) {
            phaser.register();
            exec.execute(new PubWorker(phaser, numMsgs, size));
        }

        System.out.printf("Starting benchmark [msgs=%d, msgsize=%d, pubs=%d, subs=%d]\n", numMsgs,
                size, numPubs, numSubs);

        // Wait for subscribers and publishers to finish
        phaser.arriveAndAwaitAdvance();

        // We're done. Clean up and report.
        Runtime.getRuntime().removeShutdownHook(shutdownHook);

        bench.close();
        System.out.println(bench.report());

        if (csvFileName != null) {
            List<String> csv = bench.csv();
            Path csvFile = Paths.get(csvFileName);
            Files.write(csvFile, csv, Charset.forName("UTF-8"));
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

    long backlog() {
        return sent.get() - received.get();
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
                    // hashModulo = numMsgs / 100;
                    it.remove();
                    continue;
                case "-csv":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    csvFileName = it.next();
                    it.remove();
                    continue;

                default:
                    System.err.printf("Unexpected token: '%s'\n", arg);
                    usage();
                    break;
            }
        }
    }

    /**
     * The main program executive.
     * 
     * @param args command line arguments
     */
    public static void main(String[] args) {
        try {
            new NatsBench(args).run();
        } catch (Exception e) {

            e.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }

}
