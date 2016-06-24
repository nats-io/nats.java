/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client.examples;

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
import io.nats.client.NUID;
import io.nats.client.Subscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
// import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A utility class for measuring NATS performance.
 *
 */
public class Benchmark {

    static final Logger log = LoggerFactory.getLogger(Benchmark.class);

    private String url = ConnectionFactory.DEFAULT_URL;
    private int numPubs = 1;
    private int numSubs = 0;
    private int numMsgs = 100000;
    private int hashModulo = 100000;
    String subject = "foo";
    String qgroup;
    String msgString;
    byte[] payload;
    AtomicInteger sent = new AtomicInteger();
    AtomicInteger received = new AtomicInteger();
    final String subInbox = String.format("_INBOX.%s", NUID.nextGlobal());

    ConnectionFactory cf;
    final CountDownLatch startSignal = new CountDownLatch(1);
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    static final String usageString =
            "\nUsage: java Subscriber [options] <subject> <msg>\n\nOptions:\n"
                    + "    -s, --server   <urls>            The nats server URLs (separated by comma)\n"
                    + "    -np                             Number of publishers\n"
                    + "    -ns                             Number of subscribers\n"
                    + "    -q                              Subscriber queue group\n"
                    + "    -n                              Number of messages\n";

    public Benchmark(String[] args) {
        if (args == null || args.length < 2) {
            usage();
            return;
        }
        parseArgs(args);
        cf = new ConnectionFactory(url);
        cf.setReconnectAllowed(false);
    }

    public void run() {
        final DecimalFormat formatter = new DecimalFormat("#,###");
        ArrayList<Thread> threads = new ArrayList<Thread>();
        final CountDownLatch readySignal = new CountDownLatch(numPubs + numSubs);
        final CountDownLatch doneSignal = new CountDownLatch(numPubs + numSubs);
        System.err.printf("Starting subscriber(s) on [%s]", subject);
        for (int i = 0; i < numSubs; i++) {
            System.err.printf(".");
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        runSubscriber(numMsgs, readySignal, doneSignal);
                    } catch (Exception e) {
                        log.error("Couldn't start publish thread", e);
                    }
                }
            });
            t.setName("BenchmarkSubscriber-" + i);
            t.start();
            threads.add(t);
        }
        System.err.println();


        Thread hook = new Thread(new Runnable() {
            @Override
            public void run() {
                System.err.println("\nCaught CTRL-C, shutting down gracefully...\n");
                shutdown.set(true);
                System.err.printf("Sent=%d\n", sent.get());
                System.err.printf("Received=%d\n", received.get());

            }
        });

        Runtime.getRuntime().addShutdownHook(hook);

        System.err.print("Starting publisher(s)");
        for (int i = 0; i < numPubs; i++) {
            System.err.printf(".");
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        runPublisher(numMsgs, readySignal, doneSignal);
                    } catch (Exception e) {
                        log.error("Couldn't start subscriber thread", e);
                    }
                }
            });
            t.setName("BenchmarkPublisher-" + i);
            t.start();
            threads.add(t);
        }
        System.err.println();

        try {
            readySignal.await();
        } catch (InterruptedException e) {
            log.warn("Interrupted", e);
        }

        log.info("Starting benchmark");
        log.info("msgs={} pubs={} subs={}", formatter.format(numMsgs), numPubs, numSubs);

        startSignal.countDown();
        long t0 = System.nanoTime();
        try {
            doneSignal.await();
            Runtime.getRuntime().removeShutdownHook(hook);
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted", e);
        }

        long t1 = System.nanoTime();
        long delta = t1 - t0;
        double total = (double) numMsgs * numPubs;
        if (numSubs > 0) {
            total *= (double) numSubs;
        }
        System.err.println();
        double totalTime = (double) delta / 1000000000.0;
        log.info("total msgs: {} total duration: {}s", formatter.format(total), totalTime);
        if (totalTime < 1.0) {
            log.warn("NOTE: test duration was < 1 second, results are extrapolated");
        }
        double extrapTotal = total * 1000000000 / delta;
        log.info("NATS throughput is {} msgs/sec", formatter.format(extrapTotal));
    }

    void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }


    /**
     * The publisher thread worker.
     * 
     * @param numMsgs
     * @param readySignal
     * @param doneSignal
     * @throws Exception
     */
    public void runPublisher(int numMsgs, CountDownLatch readySignal, CountDownLatch doneSignal)
            throws Exception {
        try {
            readySignal.countDown();
            startSignal.await();
            try (Connection nc = cf.createConnection()) {
                for (int i = 0; i < numMsgs; i++) {
                    sent.incrementAndGet();
                    nc.publish(subject, payload);
                    if (i % hashModulo == 0) {
                        System.err.printf("#");
                    }
                }
                nc.flush();
                doneSignal.countDown();
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted", e);
        }
    }

    public void runSubscriber(final int numMsgs, CountDownLatch readySignal,
            final CountDownLatch doneSignal) throws Exception {
        try {
            try (Connection nc = cf.createConnection()) {

                nc.setDisconnectedCallback(new DisconnectedCallback() {
                    @Override
                    public void onDisconnect(ConnectionEvent ev) {
                        System.err.println();
                        log.error("Subscriber disconnected");
                    }
                });
                nc.setClosedCallback(new ClosedCallback() {
                    @Override
                    public void onClose(ConnectionEvent ev) {
                        log.error("Subscriber connection closed");
                    }
                });
                nc.setExceptionHandler(new ExceptionHandler() {
                    @Override
                    public void onException(NATSException e) {
                        System.err.println();
                        log.error("Subscriber connection exception", e);
                        AsyncSubscription s = (AsyncSubscription) e.getSubscription();
                        log.error("Sent={}, Received={}", sent.get(), received.get());
                        log.error("Messages dropped (total) = {}", s.getDropped());
                        System.exit(-1);
                    }
                });

                try (final Subscription sub = nc.subscribe(subject, qgroup, new MessageHandler() {
                    @Override
                    public void onMessage(Message m) {
                        received.incrementAndGet();
                        if (received.get() % hashModulo == 0) {
                            System.err.printf("*");
                        }
                        if (received.get() >= numMsgs) {
                            doneSignal.countDown();
                            nc.close();
                        }
                    }
                })) {
                    nc.flush();

                    readySignal.countDown();
                    startSignal.await();

                    while (received.get() < numMsgs && !shutdown.get()) {

                    }
                    nc.setClosedCallback(null);
                    nc.setDisconnectedCallback(null);
                }
            }
            doneSignal.countDown();
        } catch (InterruptedException e) {
            log.warn("Interrupted", e);
        }
    }

    long backlog() {
        return sent.get() - received.get();
    }

    private void parseArgs(String[] args) {
        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        msgString = argList.get(argList.size() - 1);
        argList.remove(argList.size() - 1);
        payload = msgString.getBytes();

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
                    url = it.next();
                    it.remove();
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
                case "-q":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    qgroup = it.next();
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
     * @param args
     */
    public static void main(String[] args) {
        new Benchmark(args).run();
        System.exit(0);
    }

}
