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

package io.nats.examples.stability;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.examples.autobench.AutoBenchmark;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static io.nats.examples.ExampleUtils.*;
import static io.nats.examples.jetstream.NatsJsUtils.printFormatted;

public class JsMulti {
    static final String usageString =
            "\nUsage: java -cp build/libs/jnats-2.10.0.jar:build/libs/jnats-2.10.0-examples.jar io.nats.examples.stability.JsMulti requireds [optionals]\n" +
            "\n-a <action> always required, one of " +
            "\n   create - create the stream" +
            "\n   delete - delete the stream" +
            "\n   info - get the stream info" +
            "\n   pubSync - publish synchronously" +
            "\n   pubAsync - publish asynchronously" +
            "\n   subPush - push subscribe read messages (synchronously)" +
            "\n   subQueue - push subscribe read messages with queue (synchronously)" +
            "\n   subPull - pull subscribe read messages (different durable if threaded)" +
            "\n   subPullQueue - pull subscribe read messages (all with same durable)" +
            "\n-t stream, always required" +
            "\n-u subject, required most operations" +
            "\n-s serverURL, defaults to nats://localhost:4222" +
            "\n-o file|memory when creating the stream, default to memory" +
            "\n-c replicas when creating the stream, default to 1" +
            "\n-m totalMessages for pub or sub, default to 1,000,000" +
            "\n-p payloadSize for pub, default to 128" +
            "\n-d threads for pubs or subs defaults to 1" +
            "\n-n shared|individual when threading, whether to share the connection, defaults to shared" +
            "\n-k explicit|none ack policy, defaults to explicit" +
            "\n-j jitter between pubs or subs, in milliseconds, defaults to 0" +
            "\n-z round size for pubAsync, default to 100" +
            "\n   batch size for subPull, default to 100" +
            "\n\nInput numbers can be formatted for ease i.e. 1,000 1.000 1_000" +
            "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext";

    public static void main(String[] args) throws Exception {
//        TO RUN DIRECTLY FROM IDE, SET args HERE. FOR EXAMPLE:
//        String starter = "-s localhost -t multistream -u multisubject -a ";
//        args = null; // usage
//        args = (starter + "create -c 2").split(" ");
//        args = (starter + "delete").split(" ");
//        args = (starter + "info").split(" ");
//        args = (starter + "pubSync -m 10_000").split(" ");
//        args = (starter + "pubSync -m 10_000 -j 100").split(" ");
//        args = (starter + "pubSync -d 3 -m 300,000").split(" ");
//        args = (starter + "pubSync -d 3 -n individual -m 1.200.000").split(" ");
//        args = (starter + "pubAsync -m 1_000_000").split(" ");
//        args = (starter + "pubAsync -d 3 -m 1_200_000").split(" ");
//        args = (starter + "pubAsync -d 3 -n individual -m 1_200_000").split(" ");
//        args = (starter + "subPush -m 500_000").split(" ");
//        args = (starter + "subPush -d 3 -m 600_000").split(" ");
//        args = (starter + "subQueue -d 3 -m 1_200_000").split(" ");
//        args = (starter + "subQueue -d 3 -n individual -m 1_200_000").split(" ");
//        args = (starter + "subPull -d 3 -m 1,200,000").split(" ");
//        args = (starter + "subqueue -d 3 -m 1,200,000").split(" ");
//        args = (starter + "subpull -d 3 -m 1,200,000").split(" ");
//        args = (starter + "subpullqueue -d 3 -m 1,200,000").split(" ");

        Arguments a = readArgs(args);
        try {
            switch (a.action) {
                case CREATE: createStream(a); return;
                case DELETE: deleteStream(a); return;
                case INFO: infoStream(a); return;
            }
            if (a.threads > 1) {
                if (a.shared) {
                    ThreadedRunner tr;
                    switch (a.action) {
                        case PUB_SYNC:
                            tr = (js, stats, id) -> pubSync(a, js, stats, "pubSyncShared " + id);
                            break;
                        case PUB_ASYNC:
                            tr = (js, stats, id) -> pubAsync(a, js, stats, "pubAsyncShared " + id);
                            break;
                        case SUB_PUSH:
                            tr = (js, stats, id) -> subPush(a, js, stats, false, "subPushShared " + id);
                            break;
                        case SUB_QUEUE:
                            tr = (js, stats, id) -> subPush(a, js, stats, true, "subPushShared " + id);
                            break;
                        case SUB_PULL:
                            tr = (js, stats, id) -> subPull(a, js, stats, id, "subPullShared " + id);
                            break;
                        case SUB_PULL_QUEUE:
                            tr = (js, stats, id) -> subPull(a, js, stats, 0, "subPullShared " + id);
                            break;
                        default:
                            return;
                    }
                    runShared(a, tr);
                }
                else {
                    ThreadedRunner tr;
                    switch (a.action) {
                        case PUB_SYNC:
                            tr = (js, stats, id) -> pubSync(a, js, stats, "pubSyncThreadedIndividual " + id);
                            break;
                        case PUB_ASYNC:
                            tr = (js, stats, id) -> pubAsync(a, js, stats, "pubAsyncThreadedIndividual " + id);
                            break;
                        case SUB_PUSH:
                            tr = (js, stats, id) -> subPush(a, js, stats, false, "subPushIndividual " + id);
                            break;
                        case SUB_QUEUE:
                            tr = (js, stats, id) -> subPush(a, js, stats, true, "subPushIndividual " + id);
                            break;
                        case SUB_PULL:
                            tr = (js, stats, id) -> subPull(a, js, stats, id, "subPullIndividual " + id);
                            break;
                        case SUB_PULL_QUEUE:
                            tr = (js, stats, id) -> subPull(a, js, stats, 0, "subPullIndividual " + id);
                            break;
                        default:
                            return;
                    }
                    runIndividual(a, tr);
                }
            }
            else {
                SingleRunner sr;
                switch (a.action) {
                    case PUB_SYNC:
                        sr = (js, stats) -> pubSync(a, js, stats, "pubSync");
                        break;
                    case PUB_ASYNC:
                        sr = (js, stats) -> pubAsync(a, js, stats, "pubAsync");
                        break;
                    case SUB_PUSH:
                        sr = (js, stats) -> subPush(a, js, stats, false, "subPush");
                        break;
                    case SUB_QUEUE:
                        sr = (js, stats) -> subPush(a, js, stats, true, "subPushQueue");
                        break;
                    case SUB_PULL:
                    case SUB_PULL_QUEUE: // pull queue doesn't make sense without multiple
                        sr = (js, stats) -> subPull(a, js, stats, 0, "subPull");
                        break;
                    default:
                        return;
                }
                runSingle(a, sr);
            }
        }
        catch (Exception e) {
            //noinspection ThrowablePrintedToSystemOut
            System.out.println(e);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Implementation
    // ----------------------------------------------------------------------------------------------------
    private static void pubSync(Arguments a, JetStream js, Stats stats, String label) throws Exception {
        int report = a.perThread() / 100;
        for (int x = 1; x <= a.perThread(); x++) {
            stats.mark();
            js.publish(a.subject, a.getPayload());
            stats.count(a.payloadSize);
            if (x % report == 0) {
                System.out.println(label + " completed publishing " + format(x));
            }
        }
        System.out.println(label + " completed publishing");
    }

    private static void pubAsync(Arguments a, JetStream js, Stats stats, String label) {
        List<CompletableFuture<PublishAck>> futures = new ArrayList<>();
        int report = a.perThread() / 100;
        int r = a.size - 1;
        for (int x = 1; x <= a.perThread(); x++) {
            if (++r == a.size) {
                processFutures(futures, stats);
                r = 0;
            }
            stats.mark();
            futures.add(js.publishAsync(a.subject, a.getPayload()));
            stats.count(a.payloadSize);
            if (x % report == 0) {
                System.out.println(label + " completed publishing " + x);
            }
        }
        System.out.println(label + " completed publishing");
    }

    private static void processFutures(List<CompletableFuture<PublishAck>> futures, Stats stats) {
        while (futures.size() > 0) {
            stats.mark();
            CompletableFuture<PublishAck> f = futures.remove(0);
            if (!f.isDone()) {
                futures.add(f);
            }
            stats.time();
        }
    }

    private static void subPush(Arguments a, JetStream js, Stats stats, boolean q, String label) throws Exception {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackPolicy(a.ack ? AckPolicy.Explicit : AckPolicy.None)
                .build();
        PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
        JetStreamSubscription sub = q ? js.subscribe(a.subject, "q" + uniqueEnough(), pso) : js.subscribe(a.subject, pso);
        int report = a.perThread() / 100;
        int x = 0;
        while (x < a.perThread()) {
            a.jitter();
            stats.mark();
            Message m = sub.nextMessage(Duration.ofSeconds(1));
            if (m == null) {
                stats.time();
                break;
            }
            if (m.isJetStream()) {
                if (a.ack) {
                    m.ack();
                }
                stats.count(m.getData().length);
                x++;
                if (x % report == 0) {
                    System.out.println(label + " messages read " + format(x));
                }
            }
            else {
                stats.time();
            }
        }
        System.out.println(label + " finished messages read " + format(x));
    }

    private static void subPull(Arguments a, JetStream js, Stats stats, int durableId, String label) throws Exception {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackPolicy(a.ack ? AckPolicy.Explicit : AckPolicy.None)
                .build();
        PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(uniqueEnough() + durableId).configuration(cc).build();
        JetStreamSubscription sub = js.subscribe(a.subject, pso);

        int report = a.perThread() / 100;
        int x = 0;
        while (x < a.perThread()) {
            stats.mark();
            Iterator<Message> iter = sub.iterate(a.size, Duration.ofSeconds(1));
            stats.time();
            while (iter.hasNext()) {
                stats.mark();
                Message m = iter.next();
                if (a.ack) {
                    m.ack();
                }
                stats.count(m.getData().length);
                x++;
                if (x % report == 0) {
                    System.out.println(label + " messages read " + format(x));
                }
                a.jitter();
            }
        }
        System.out.println(label + " finished messages read " + format(x));
    }

    private static void createStream(Arguments a) throws Exception {
        try (Connection nc = connect(a)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamConfiguration.Builder builder = StreamConfiguration.builder()
                    .name(a.stream)
                    .subjects(a.subject)
                    .storageType(a.file ? StorageType.File : StorageType.Memory);

            if (a.replicas > 1) {
                builder.replicas(a.replicas);
            }

            jsm.addStream(builder.build());
        }
    }

    private static void deleteStream(Arguments a) throws Exception {
        try (Connection nc = connect(a)) {
            nc.jetStreamManagement().deleteStream(a.stream);
        }
    }

    private static void infoStream(Arguments a) throws Exception {
        try (Connection nc = connect(a)) {
            printFormatted(nc.jetStreamManagement().getStreamInfo(a.stream));
        }
    }

    private static Connection connect(Arguments a) throws Exception {
        Options options = createExampleOptions(a.server, true);
        Connection nc = Nats.connect(options);
        for (long x = 0; x < 100; x++) { // waits up to 10 seconds (100 * 100 = 10000) millis to be connected
            sleep(100);
            if (nc.getStatus() == Connection.Status.CONNECTED) {
                return nc;
            }
        }
        return nc;
    }

    // ----------------------------------------------------------------------------------------------------
    // Runners
    // ----------------------------------------------------------------------------------------------------
    interface SingleRunner {
        void run(JetStream js, Stats stats) throws Exception;
    }

    interface ThreadedRunner {
        void run(JetStream js, Stats stats, int id) throws Exception;
    }

    private static void runSingle(Arguments a, SingleRunner runner) throws Exception {
        Stats stats = new Stats();
        try (Connection nc = connect(a)) {
            runner.run(nc.jetStream(), stats);
        }
        reportStats(stats, "Total");
    }

    private static void runShared(Arguments a, ThreadedRunner runner) throws Exception {
        List<Thread> threads = new ArrayList<>();
        List<Stats> statss = new ArrayList<>();
        try (Connection nc = connect(a)) {
            final JetStream js = nc.jetStream();
            for (int x = 0; x < a.threads; x++) {
                final int id = x + 1;
                final Stats stats = new Stats();
                Thread t = new Thread(() -> {
                    try {
                        runner.run(js, stats, id);
                    } catch (Exception e) {
                        System.out.println("\n Error in thread " + id);
                        e.printStackTrace();
                    }
                });
                statss.add(stats);
                threads.add(t);
            }
            for (Thread t : threads) {
                t.start();
            }
            for (Thread t : threads) {
                t.join();
            }
        }
        reportStats(statss);
    }

    private static void runIndividual(Arguments a, ThreadedRunner runner) throws Exception {
        List<Thread> threads = new ArrayList<>();
        List<Stats> statss = new ArrayList<>();
        for (int x = 0; x < a.threads; x++) {
            final int id = x + 1;
            final Stats stats = new Stats();
            Thread t = new Thread(() -> {
                try (Connection nc = connect(a)) {
                    runner.run(nc.jetStream(), stats, id);
                } catch (Exception e) {
                    System.out.println("\n Error in thread " + id);
                    e.printStackTrace();
                }
            });
            statss.add(stats);
            threads.add(t);
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        reportStats(statss);
    }

    // ----------------------------------------------------------------------------------------------------
    // General Utility
    // ----------------------------------------------------------------------------------------------------
    private static int toInt(String s) {
        return Integer.parseInt(normalize(s));
    }

    private static long toLong(String s) {
        return Long.parseLong(normalize(s));
    }

    private static String normalize(String s) {
        return s.replaceAll("_", "").replaceAll(",", "").replaceAll("\\.", "");
    }

    private static String format(Number s) {
        return NumberFormat.getNumberInstance(Locale.getDefault()).format(s);
    }

    static class Stats {
        long elapsed = 0;
        long bytes = 0;
        int messageCount = 0;
        long now;

        void mark() {
            now = System.nanoTime();
        }

        void count(long bytes) {
            elapsed += System.nanoTime() - now;
            messageCount++;
            this.bytes += bytes;
        }

        void time() {
            elapsed += System.nanoTime() - now;
        }
    }

    private static void reportStats(Stats stats, String label) {
        double elapsed = stats.elapsed / 1e6;
        double messagesPerSecond = stats.elapsed == 0 ? 0 : stats.messageCount * 1e9 / stats.elapsed;
        double bytesPerSecond = 1e9 * ((double)stats.bytes)/((double)stats.elapsed);
        System.out.printf("%-10s | %12s msgs | %12s ms | %10s msg/s | %10s/s\n", label,
                format(stats.messageCount),
                format(elapsed),
                format((long)messagesPerSecond),
                AutoBenchmark.humanBytes(bytesPerSecond));
    }

    private static void reportStats(List<Stats> statss) {
        Stats total = new Stats();
        int x = 0;
        for (Stats stats : statss) {
            reportStats(stats, "Thread " + (++x));
            total.elapsed += stats.elapsed;
            total.messageCount += stats.messageCount;
            total.bytes += stats.bytes;
        }

        reportStats(total, "Total");
    }

    public static String humanBytes(double bytes) {
        int base = 1024;
        String[] pre = new String[] {"k", "m", "g", "t", "p", "e"};
        String post = "b";
        if (bytes < (long) base) {
            return String.format("%.2f b", bytes);
        }
        int exp = (int) (Math.log(bytes) / Math.log(base));
        int index = exp - 1;
        String units = pre[index] + post;
        return String.format("%.2f %s", bytes / Math.pow((double) base, (double) exp), units);
    }

    // ----------------------------------------------------------------------------------------------------
    // Arguments
    // ----------------------------------------------------------------------------------------------------
    static class Arguments {
        String server = Options.DEFAULT_URL;
        String action;
        int totalMsgs = 1_000_000;
        int payloadSize = 128;
        int size = 100;
        int replicas = 1;
        int threads = 1;
        boolean file = false;
        boolean ack = true;
        boolean shared = true;
        String stream;
        String subject;
        long jitter = 0;

        private byte[] _payload;
        private byte[] getPayload() {
            if (_payload == null) {
                _payload = new byte[payloadSize];
            }
            return _payload;
        }

        private int perThread() {
            return totalMsgs / threads;
        }

        private void jitter() {
            if (jitter > 0) {
                sleep(ThreadLocalRandom.current().nextLong(jitter));
            }
        }

        @Override
        public String toString() {
            return "JsMulti Run Config:" +
                    "\n  action (-a):      " + action +
                    "\n  server (-s):      " + server +
                    "\n  stream (-t):      " + stream +
                    "\n  subject (-u):     " + subject +
                    "\n  totalMsgs (-m):   " + format(totalMsgs) +
                    "\n  payload (-p):     " + payloadSize + " bytes" +
                    "\n  size (-z):        " + format(size) +
                    "\n  jitter (-j):      " + jitter +
                    "\n  replicas (-c):    " + replicas +
                    "\n  threads (-d):     " + threads +
                    "\n  file (-f):        " + (file ? StorageType.File : StorageType.Memory) +
                    "\n  ack (-k):         " + (ack ? AckPolicy.Explicit : AckPolicy.None) +
                    "\n  connection (-n):  " + (shared ? "shared" : "individual")
                    ;
        }
    }

    private static Arguments readArgs(String[] args) {
        boolean unknown = false;
        Arguments a = new Arguments();

        if (args != null && args.length > 0) {
            for (int x = 0; x < args.length; x++) {
                switch (args[x]) {
                    case "-s":
                        a.server = args[++x];
                        break;
                    case "-a":
                        a.action = args[++x].toLowerCase();
                        break;
                    case "-t":
                        a.stream = args[++x];
                        break;
                    case "-u":
                        a.subject = args[++x];
                        break;
                    case "-m":
                        a.totalMsgs = toInt(args[++x]);
                        break;
                    case "-p":
                        a.payloadSize = toInt(args[++x]);
                        break;
                    case "-z":
                        a.size = toInt(args[++x]);
                        break;
                    case "-d":
                        a.threads = toInt(args[++x]);
                        break;
                    case "-c":
                        a.replicas = toInt(args[++x]);
                        break;
                    case "-j":
                        a.jitter = toLong(args[++x]);
                        break;
                    case "-n":
                        a.shared = !args[++x].equalsIgnoreCase("individual");
                        break;
                    case "-o":
                        a.file = args[++x].equalsIgnoreCase("file");
                        break;
                    case "-k":
                        a.ack = args[++x].equalsIgnoreCase("explicit");
                        break;
                    default:
                        unknown = true;
                        break;
                }
            }
        }
        System.out.println(a + "\n");

        if (unknown || a.action == null || a.stream == null || !ALL_ACTIONS.contains(a.action)) {
            System.err.println(usageString);
            System.exit(-1);
        }

        if (a.subject == null && SUBJECT_ACTIONS.contains(a.action)) {
            System.err.println(usageString);
            System.exit(-1);
        }

        return a;
    }

    static final String ALL_ACTIONS = "|create|delete|info|pubsync|pubasync|subpush|subqueue|subpull|subpullqueue|";
    static final String SUBJECT_ACTIONS = "|create||pubsync|pubasync|subpush|subqueue|subpull|subpullqueue|";
    static final String CREATE = "create";
    static final String DELETE = "delete";
    static final String INFO = "info";
    static final String PUB_SYNC = "pubsync";
    static final String PUB_ASYNC = "pubasync";
    static final String SUB_PUSH = "subpush";
    static final String SUB_QUEUE = "subqueue";
    static final String SUB_PULL = "subpull";
    static final String SUB_PULL_QUEUE = "subpullqueue";
}
