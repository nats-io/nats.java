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

package io.nats.examples.benchmark;

import io.nats.client.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A utility class for measuring NATS performance, similar to the version in go
 * and node. The various tradeoffs to make this code act/work like the other
 * versions, including the previous java version, make it a bit
 * &quot;crufty&quot; for an example. See autobench for an example with minimal
 * boilerplate.
 */
public class NatsBench2 {
    final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<Throwable>();

    // Default test values
    private int numMsgs = 5_000_000;
    private int numPubs = 1;
    private int numSubs = 0;
    private int size = 128;

    private String urls = Options.DEFAULT_URL;
    private String subject;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong received = new AtomicLong();
    private boolean csv = false;
    private boolean stats = false;
    private boolean conscrypt = false;

    private Thread shutdownHook;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private boolean secure = false;
    private Benchmark bench;
    private long succPubMsgCount;
    private long succSubMsgCount;
    
    static final String usageString = "\nUsage: java -cp <classpath> NatsBench [-s server] [-tls] [-np num] [-ns num] [-n num] [-ms size] "
            + "[-csv file] <subject>\n\nOptions:\n"
            + "    -s  <urls>                     The nats server URLs (comma-separated), use tls:// or opentls:// to require tls\n"
            + "    -np <int>                       Number of concurrent publishers (1)\n"
            + "    -ns <int>                       Number of concurrent subscribers (0)\n"
            + "    -n  <int>                       Number of messages to publish (100,000)\n"
            + "    -ms <int>                       Size of the message (128)\n"
            + "    -csv                            Print results to stdout as csv (false)\n"
            + "    -tls                            Set the secure flag on the SSL context to true (false)\n"
            + "    -stats                          Track and print out internal statistics (false)\n";

    public NatsBench2(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            usage();
            return;
        }
        parseArgs(args);
    }

    public NatsBench2(Properties properties) throws NoSuchAlgorithmException {
        urls = properties.getProperty("bench.nats.servers", urls);
        secure = Boolean.parseBoolean(properties.getProperty("bench.nats.secure", Boolean.toString(secure)));
        numMsgs = Integer.parseInt(properties.getProperty("bench.nats.msg.count", Integer.toString(numMsgs)));
        size = Integer.parseInt(properties.getProperty("bench.nats.msg.size", Integer.toString(numSubs)));
        numPubs = Integer.parseInt(properties.getProperty("bench.nats.pubs", Integer.toString(numPubs)));
        numSubs = Integer.parseInt(properties.getProperty("bench.nats.subs", Integer.toString(numSubs)));
        csv = Boolean.parseBoolean(properties.getProperty("bench.nats.csv", Boolean.toString(csv)));
        subject = properties.getProperty("bench.nats.subject", NUID.nextGlobal());
    }

    Options prepareOptions(boolean secure) throws NoSuchAlgorithmException {
        String[] servers = urls.split(",");
        Options.Builder builder = new Options.Builder();
        //builder.noReconnect();
        builder.maxReconnects(-1);
        builder.connectionName("NatsBench");
        builder.servers(servers);
        builder.connectionListener(new ConnectionListener() {

            @Override
            public void connectionEvent(Connection conn, Events type) {
                System.out.println("Connection Event:" + type);
                if (type == Events.DISCOVERED_SERVERS)
                {
                    conn.getServers().forEach(System.out::println);
                }
                if (type == Events.RECONNECTED)
                {
                    System.out.println("Reconnected to:" + conn.getConnectedUrl());
                }
            }

        });
        builder.errorListener(new ErrorListener() {
            @Override
            public void errorOccurred(Connection conn, String error) {
                System.out.printf("An error occurred %s\n", error);
            }

            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                System.out.println("An exception occurred...");
                exp.printStackTrace();
            }

            @Override
            public void slowConsumerDetected(Connection conn, Consumer consumer) {
                System.out.println("Slow consumer detected");
            }
        });

        if (stats) {
            builder.turnOnAdvancedStats();
        }

        /**
         * The conscrypt flag is provided for testing with the conscrypt jar. Using it
         * through reflection is deprecated but allows the library to ship without a
         * dependency. Using conscrypt should only require the jar plus the flag. For
         * example, to run after building locally and using the test cert files: java
         * -cp
         * ./build/libs/jnats-2.5.1-SNAPSHOT-examples.jar:./build/libs/jnats-2.5.1-SNAPSHOT-fat.jar:<path
         * to conscrypt.jar> \ -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks
         * -Djavax.net.ssl.keyStorePassword=password \
         * -Djavax.net.ssl.trustStore=src/test/resources/truststore.jks
         * -Djavax.net.ssl.trustStorePassword=password \
         * io.nats.examples.autobench.NatsAutoBench tls://localhost:4443 med conscrypt
         */
        if (conscrypt) {
            try {
                Provider provider = null;
                provider = (Provider) Class.forName("org.conscrypt.OpenSSLProvider").newInstance();
                Security.insertProviderAt(provider, 1);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

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
        final Phaser subReady;
        private AtomicLong start;

        SyncSubWorker(Future<Boolean> starter, Phaser subReady, Phaser finisher, int numMsgs, int size, boolean secure) {
            super(starter, finisher, numMsgs, size, secure);
            this.subReady = subReady;
            this.start = new AtomicLong();
        }

        @Override
        public void run() {
            try {
                Options opts = prepareOptions(this.secure);
                Connection nc = Nats.connect(opts);

                Subscription sub = nc.subscribe(subject);
                nc.flush(null);

                // Signal we are ready
                subReady.arrive();

                // Wait for the signal to start tracking time
                starter.get(60, TimeUnit.SECONDS);

                Duration timeout = Duration.ofMillis(1000);

                int receivedCount = 0;
                while (receivedCount < numMsgs) {
                    if(sub.nextMessage(timeout) != null) {
                        if (receivedCount == 0) {
                            start.set(System.nanoTime());
                        }
                        received.incrementAndGet();
                        receivedCount++;
                    }
                }
                long end = System.nanoTime();

                if (start.get() > 0) {
                    bench.addSubSample(new Sample(numMsgs, size, start.get(), end, nc.getStatistics()));
                } else {
                    throw new Exception("start time was never set");
                }

                if (stats) {
                    System.out.println(nc.getStatistics());
                }

                // Clean up
                sub.unsubscribe();
                succSubMsgCount = receivedCount;
                nc.close();
            } catch (Exception e) {
                errorQueue.add(e);
            } finally {
                subReady.arrive();
                finisher.arrive();
            }
        }
    }

    class PubWorker extends Worker {

        private AtomicLong start;

        PubWorker(Future<Boolean> starter, Phaser finisher, int numMsgs, int size, boolean secure) {
            super(starter, finisher, numMsgs, size, secure);
            this.start = new AtomicLong();
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
                starter.get(60, TimeUnit.SECONDS);
                start.set(System.nanoTime());
                for (int i = 0; i < numMsgs; i++) {

                    boolean success = false ;
                    
                    for (int idx = 5; idx < 10 && success == false; idx++) {
                        try {
                            nc.publish(subject, payload);
                            success = true;
                        } catch (IllegalStateException ex) {
                            if (ex.getMessage().contains("Output queue is full")) {
                                success = false; 
                                Thread.sleep(1000);
                            } else {
                                throw ex; 
                            }
                        }
                    }
                    //Thread.sleep(100);
                    sent.incrementAndGet();
                }
                nc.flush(Duration.ofSeconds(15));
                long end = System.nanoTime();

                bench.addPubSample(new Sample(numMsgs, size, start.get(), end, nc.getStatistics()));

                if (stats) {
                    System.out.println(nc.getStatistics());
                }
                //succPubMsgCount += ((io.nats.client.impl.NatsStatistics)nc.getStatistics()).getOKs();
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
        System.out.printf("Current memory usage is %s / %s / %s free/total/max\n", 
                            Utils.humanBytes(Runtime.getRuntime().freeMemory()),
                            Utils.humanBytes(Runtime.getRuntime().totalMemory()),
                            Utils.humanBytes(Runtime.getRuntime().maxMemory()));
        System.out.println("Use ctrl-C to cancel.");
        System.out.println();

        if (this.numPubs > 0 && this.numSubs > 0) {
            //runTest("Pub Only", this.numPubs, 0);
            runTest("Pub/Sub", this.numPubs, this.numSubs);
        } else if (this.numPubs > 0) {
            runTest("Pub Only", this.numPubs, 0);
        } else {
            runTest("Sub Only", 0, this.numSubs);
        }

        System.out.println();
        System.out.println("Successfully Published messages: " + getSuccPubMsgCount());
        System.out.println("Successfully Received messages: " + getSuccResMsgCount());
        System.out.printf("Final memory usage is %s / %s / %s free/total/max\n", 
                            Utils.humanBytes(Runtime.getRuntime().freeMemory()),
                            Utils.humanBytes(Runtime.getRuntime().totalMemory()),
                            Utils.humanBytes(Runtime.getRuntime().maxMemory()));
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }

    public void runTest(String title, int pubCount, int subCount) throws Exception {
        final Phaser subReady = new Phaser();
        final Phaser finisher = new Phaser();
        final CompletableFuture<Boolean> starter = new CompletableFuture<>();
        subReady.register();
        finisher.register();
        sent.set(0);
        received.set(0);

        bench = new Benchmark(title);

        // Run Subscribers first
        for (int i = 0; i < subCount; i++) {
            subReady.register();
            finisher.register();
            new Thread(new SyncSubWorker(starter, subReady, finisher, this.numMsgs, this.size, secure), "Sub-"+i).start();
        }

        // Wait for subscribers threads to initialize
        subReady.arriveAndAwaitAdvance();

        if (!errorQueue.isEmpty()) {
            Throwable error = errorQueue.take();
            System.err.printf(error.getMessage());
            error.printStackTrace();
            throw new RuntimeException(error);
        }

        // Now publishers
        if (pubCount != 0) { // running pub in another app
            int remaining = this.numMsgs;
            int perPubMsgs = this.numMsgs / pubCount;
            for (int i = 0; i < pubCount; i++) {
                finisher.register();
                if (i == numPubs - 1) {
                    perPubMsgs = remaining;
                }
                
                if (subCount == 0) {
                    new Thread(new PubWorker(starter, finisher, perPubMsgs, this.size, secure), "Pub-"+i).start();
                } else {
                    new Thread(new PubWorker(starter, finisher, perPubMsgs, this.size, secure), "Pub-"+i).start();
                }
                
                remaining -= perPubMsgs;
            }
        } else {
            System.out.println("Starting subscribers, time to run the publishers somewhere ...");
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
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
            throw new RuntimeException(error);
        }

        if (subCount==1 && pubCount>0 && sent.get() != received.get()) {
            System.out.println("#### Error - sent and received are not equal "+sent.get() + " != " + received.get());
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
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    urls = it.next();
                    it.remove();
                    continue;
                case "-tls":
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
                    it.remove();
                    csv = true;
                    continue;
                case "-stats":
                    it.remove();
                    stats = true;
                    continue;
                case "-conscrypt":
                    it.remove();
                    conscrypt = true;
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
        Properties properties = null;
        try {
            if (args.length == 1 && args[0].endsWith(".properties")) {
                properties = loadProperties(args[0]);
                new NatsBench2(properties).start();
            } else {
                new NatsBench2(args).start();
            }
        } catch (Exception e) {
            System.err.printf("Exiting due to exception [%s]\n", e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }
    
    public long getSuccPubMsgCount() {
    	return this.succPubMsgCount;
    }
    
    public long getSuccResMsgCount() {
    	return this.succSubMsgCount;
    }

}
