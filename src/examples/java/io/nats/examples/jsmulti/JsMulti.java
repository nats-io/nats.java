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

package io.nats.examples.jsmulti;

import io.nats.client.*;
import io.nats.client.api.*;

import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static io.nats.client.support.JsonUtils.printFormatted;
import static io.nats.examples.ExampleUtils.*;

public class JsMulti {
    static final String usageString =
            "\nUsage: java -cp build/libs/jnats-2.11.1.jar:build/libs/jnats-2.11.1-examples.jar \\"
                    + "\n       io.nats.examples.jsmulti.JsMulti configurations\n"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\nConfiguration Options"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-a action (string), required, one of "
                    + "\n   create       - create the stream"
                    + "\n   delete       - delete the stream"
                    + "\n   info         - get the stream info"
                    + "\n   pubSync      - publish synchronously"
                    + "\n   pubAsync     - publish asynchronously"
                    + "\n   pubCore      - core publish (synchronously) to subject"
                    + "\n   subPush      - push subscribe read messages (synchronously)"
                    + "\n   subQueue     - push subscribe read messages with queue (synchronously). "
                    + "\n                    Requires 2 or more threads"
                    + "\n   subPull      - pull subscribe read messages"
                    + "\n   subPullQueue - pull subscribe read messages, queue (using common durable)"
                    + "\n                    Requires 2 or more threads"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-s server url (string), optional, defaults to nats://localhost:4222"
                    + "\n-rf report frequency (number) how often to print progress, defaults to 1000 messages."
                    + "\n    <= 0 for no reporting. Reporting time is excluded from timings"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-t stream (string), required for stream operations"
                    + "\n-o storage type (file|memory) when creating a stream, defaults to memory"
                    + "\n-c replicas (number) when creating a stream, defaults to 1, maximum 5"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-u subject (string), required for publishing or subscribing"
                    + "\n-m message count (number) for publishing or subscribing, defaults to 1 million"
                    + "\n-d threads (number) for publishing or subscribing, defaults to 1"
                    + "\n-n connection strategy (shared|individual) when threading, whether to share"
                    + "\n     the connection, defaults to shared"
                    + "\n-j jitter (number) between publishes or subscribe message retrieval of random"
                    + "\n     number from 0 to j-1, in milliseconds, defaults to 0 (no jitter), maximum 10_000"
                    + "\n     time spent in jitter is excluded from timings"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-ps payload size (number) for publishing, defaults to 128, maximum 8192"
                    + "\n-rs round size (number) for pubAsync, default to 100, maximum 1000"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\n-pt pull type (fetch|iterate), fetch all first, or iterate, defaults to iterate."
                    + "\n      Ack policy must be explicit."
                    + "\n-kp ack policy (explicit|none|all) for subscriptions, defaults to explicit"
                    + "\n-kf ack frequency (number), applies to ack policy all, ack after kf messages"
                    + "\n      defaults to 1, maximum 256"
                    + "\n-bs batch size (number) for subPull/subPullQueue, defaults to 10, maximum 256"
                    + "\n---------------------------------------------------------------------------------------"
                    + "\nInput numbers can be formatted for easier viewing. For instance, ten thousand"
                    + "\n  can be any of these: 10000 10,000 10.000 10_000"
                    + "\nUse tls:// or opentls:// in the server url to require tls, via the Default SSLContext"
                    + "\n---------------------------------------------------------------------------------------"
            ;

    public static void main(String[] args) throws Exception {
//        TO RUN DIRECTLY FROM IDE, SET args HERE. FOR EXAMPLE:
//        usage ... don't set any args
//        general ...
//        args = "-s localhost:4222 -t stream -u subject -a action ...".split(" ");
//        stream ...
//        args = "-a create -t multistream".split(" ");
//        args = "-a delete -t multistream".split(" ");
//        args = "-a info   -t multistream".split(" ");
//        publish ...
//        args = "-a pubSync      -u multisubject        -m 10_000".split(" ");
//        args = "-a pubSync      -u multisubject        -m 10_000  -j 100".split(" ");
//        args = "-a pubSync      -u multisubject  -d 3  -m 300,000".split(" ");
//        args = "-a pubSync      -u multisubject  -d 4  -m 200.000 -rf 5000 -n individual".split(" ");
//        args = "-a pubAsync     -u multisubject        -m 1_000_000".split(" ");
//        args = "-a pubAsync     -u multisubject  -d 3  -m 1_200_000".split(" ");
//        args = "-a pubAsync     -u multisubject  -d 3  -m 1_200_000  -n individual".split(" ");
//        args = "-a pubcore      -u multisubject    -rf -1 -d 4    -m 1_000_000".split(" ");
//        subscribe ...
//        args = "-a subPush      -u multisubject        -m 500_000".split(" ");
//        args = "-a subPush      -u multisubject  -d 3  -m 600_000".split(" ");
//        args = "-a subQueue     -u multisubject  -d 3  -m 1_200_000".split(" ");
//        args = "-a subQueue     -u multisubject  -d 3  -m 1_200_000  -n individual".split(" ");
//        args = "-a subPull      -u multisubject  -d 3  -m 1,200,000".split(" ");
//        args = "-a subqueue     -u multisubject  -d 3  -m 1,200,000".split(" ");
//        args = "-a subpull      -u multisubject  -d 3  -m 1,200,000".split(" ");
//        args = "-a subpullqueue -u multisubject  -d 3  -m 1,200,000".split(" ");

        Arguments a = readArgs(args);
        try {
            switch (a.action) {
                case CREATE:
                    createStream(a);
                case INFO:
                    infoStream(a);
                    return;
                case DELETE:
                    deleteStream(a);
                    return;
            }
            if (a.threads > 1) {
                if (a.shared) {
                    ThreadedRunner tr;
                    switch (a.action) {
                        case PUB_CORE:
                            tr = (nc, js, stats, id) -> pubCore(a, nc, stats, "pubCoreShared " + id);
                            break;
                        case PUB_SYNC:
                            tr = (nc, js, stats, id) -> pubSync(a, js, stats, "pubSyncShared " + id);
                            break;
                        case PUB_ASYNC:
                            tr = (nc, js, stats, id) -> pubAsync(a, js, stats, "pubAsyncShared " + id);
                            break;
                        case SUB_PUSH:
                            tr = (nc, js, stats, id) -> subPush(a, js, stats, false, "subPushShared " + id);
                            break;
                        case SUB_QUEUE:
                            tr = (nc, js, stats, id) -> subPush(a, js, stats, true, "subPushSharedQueue " + id);
                            break;
                        case SUB_PULL:
                            tr = (nc, js, stats, id) -> subPull(a, js, stats, id, "subPullShared " + id);
                            break;
                        case SUB_PULL_QUEUE:
                            tr = (nc, js, stats, id) -> subPull(a, js, stats, 0, "subPullSharedQueue " + id);
                            break;
                        default:
                            return;
                    }
                    runShared(a, tr);
                }
                else {
                    ThreadedRunner tr;
                    switch (a.action) {
                        case PUB_CORE:
                            tr = (nc, js, stats, id) -> pubCore(a, nc, stats, "pubCoreIndividual " + id);
                            break;
                        case PUB_SYNC:
                            tr = (nc, js, stats, id) -> pubSync(a, js, stats, "pubSyncIndividual " + id);
                            break;
                        case PUB_ASYNC:
                            tr = (nc, js, stats, id) -> pubAsync(a, js, stats, "pubAsyncIndividual " + id);
                            break;
                        case SUB_PUSH:
                            tr = (nc, js, stats, id) -> subPush(a, js, stats, false, "subPushIndividual " + id);
                            break;
                        case SUB_QUEUE:
                            tr = (nc, js, stats, id) -> subPush(a, js, stats, true, "subPushIndividualQueue " + id);
                            break;
                        case SUB_PULL:
                            tr = (nc, js, stats, id) -> subPull(a, js, stats, id, "subPullIndividual " + id);
                            break;
                        case SUB_PULL_QUEUE:
                            tr = (nc, js, stats, id) -> subPull(a, js, stats, 0, "subPullIndividualQueue " + id);
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
                    case PUB_CORE:
                        sr = (nc, js, stats) -> pubCore(a, nc, stats, "pubCore");
                        break;
                    case PUB_SYNC:
                        sr = (nc, js, stats) -> pubSync(a, js, stats, "pubSync");
                        break;
                    case PUB_ASYNC:
                        sr = (nc, js, stats) -> pubAsync(a, js, stats, "pubAsync");
                        break;
                    case SUB_PUSH:
                        sr = (nc, js, stats) -> subPush(a, js, stats, false, "subPush");
                        break;
                    case SUB_PULL:
                        sr = (nc, js, stats) -> subPull(a, js, stats, 0, "subPull");
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
    private static void pubCore(Arguments a, final Connection nc, Stats stats, String label) throws Exception {
        _pub(a, stats, label, () -> nc.publish(a.subject, a.getPayload()));
    }

    private static void pubSync(Arguments a, final JetStream js, Stats stats, String label) throws Exception {
        _pub(a, stats, label, () -> js.publish(a.subject, a.getPayload()));
    }

    interface Publisher {
        void publish() throws Exception;
    }

    private static void _pub(Arguments a, Stats stats, String label, Publisher p) throws Exception {
        for (int x = 1; x <= a.perThread(); x++) {
            jitter(a);
            stats.start();
            p.publish();
            stats.stopAndCount(a.payloadSize);
            reportMaybe(a, x, label, "completed publishing");
        }
        System.out.println(label + " completed publishing");
    }

    private static void pubAsync(Arguments a, JetStream js, Stats stats, String label) {
        List<CompletableFuture<PublishAck>> futures = new ArrayList<>();
        int r = 0;
        for (int x = 1; x <= a.perThread(); x++) {
            if (++r >= a.roundSize) {
                processFutures(futures, stats);
                r = 0;
            }
            jitter(a);
            stats.start();
            futures.add(js.publishAsync(a.subject, a.getPayload()));
            stats.stopAndCount(a.payloadSize);
            reportMaybe(a, x, label, "completed publishing");
        }
        System.out.println(label + " completed publishing");
    }

    private static void processFutures(List<CompletableFuture<PublishAck>> futures, Stats stats) {
        stats.start();
        while (futures.size() > 0) {
            CompletableFuture<PublishAck> f = futures.remove(0);
            if (!f.isDone()) {
                futures.add(f);
            }
        }
        stats.stop();
    }

    private static void subPush(Arguments a, JetStream js, Stats stats, boolean q, String label) throws Exception {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackPolicy(a.ackPolicy)
                .build();
        PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
        JetStreamSubscription sub = q ? js.subscribe(a.subject, "q" + uniqueEnough(), pso) : js.subscribe(a.subject, pso);
        int x = 0;
        List<Message> ackList = new ArrayList<>();
        while (x < a.perThread()) {
            jitter(a);
            stats.start();
            Message m = sub.nextMessage(Duration.ofSeconds(1));
            stats.stop();
            if (m == null) {
                break;
            }
            if (m.isJetStream()) {
                stats.count(m.getData().length);
                ackMaybe(a, stats, ackList, m);
                reportMaybe(a, ++x, label, "messages read");
            }
            else {
                stats.stop();
            }
        }
        ackEm(stats, ackList, 1);
        System.out.println(label + " finished messages read " + format(x));
    }

    private static void ackMaybe(Arguments a, Stats stats, List<Message> ackList, Message m) {
        if (a.ackFrequency < 2) {
            stats.start();
            m.ack();
            stats.stop();
        }
        else {
            switch (a.ackPolicy) {
                case Explicit:
                    ackList.add(m);
                    break;
                case All:
                    if (ackList.isEmpty()) {
                        ackList.add(m);
                    }
                    else {
                        ackList.set(0, m);
                    }
                    break;
            }
            ackEm(stats, ackList, a.ackFrequency);
        }
    }

    private static void ackEm(Stats stats, List<Message> ackList, int thresh) {
        if (ackList.size() >= thresh) {
            stats.start();
            for (Message m : ackList) {
                m.ack();
            }
            stats.stop();
            ackList.clear();
        }
    }

    private static void subPull(Arguments a, JetStream js, Stats stats, int durableId, String label) throws Exception {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .ackPolicy(a.ackPolicy)
                .build();
        PullSubscribeOptions pso = PullSubscribeOptions.builder().durable(uniqueEnough() + durableId).configuration(cc).build();
        JetStreamSubscription sub = js.subscribe(a.subject, pso);

        if (a.pullTypeIterate) {
            System.out.println("ITERATE");
            subPullIterate(a, stats, label, sub);
        }
        else {
            System.out.println("FETCHING");
            subPullFetch(a, stats, label, sub);
        }
    }

    private static void subPullIterate(Arguments a, Stats stats, String label, JetStreamSubscription sub) {
        int x = 0;
        List<Message> ackList = new ArrayList<>();
        while (x < a.perThread()) {
            jitter(a);
            stats.start();
            Iterator<Message> iter = sub.iterate(a.batchSize, Duration.ofSeconds(1));
            stats.stop();
            Message m;
            while (iter.hasNext()) {
                stats.start();
                m = iter.next();
                stats.stopAndCount(m.getData().length);
                ackMaybe(a, stats, ackList, m);
                reportMaybe(a, ++x, label, "messages read");
            }
        }
        ackEm(stats, ackList, 1);
        System.out.println(label + " finished messages read " + format(x));
    }

    private static void subPullFetch(Arguments a, Stats stats, String label, JetStreamSubscription sub) {
        int x = 0;
        List<Message> ackList = new ArrayList<>();
        while (x < a.perThread()) {
            jitter(a);
            stats.start();
            List<Message> list = sub.fetch(a.batchSize, Duration.ofSeconds(1));
            stats.stop();
            for (Message m : list) {
                ackMaybe(a, stats, ackList, m);
                reportMaybe(a, ++x, label, "messages read");
            }
        }
        ackEm(stats, ackList, 1);
        System.out.println(label + " finished messages read " + format(x));
    }

    private static void reportMaybe(Arguments a, int x, String label, String message) {
        if (x % a.reportFrequency == 0) {
            System.out.println(label + " " + message + " " + x);
        }
    }

    private static void jitter(Arguments a) {
        if (a.jitter > 0) {
            sleep(ThreadLocalRandom.current().nextLong(a.jitter));
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Stream Management
    // ----------------------------------------------------------------------------------------------------
    private static void createStream(Arguments a) throws Exception {
        try (Connection nc = connect(a)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamConfiguration.Builder builder = StreamConfiguration.builder()
                    .name(a.stream)
                    .subjects(a.subject)
                    .storageType(a.storageType);

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
        void run(Connection nc, JetStream js, Stats stats) throws Exception;
    }

    interface ThreadedRunner {
        void run(Connection nc, JetStream js, Stats stats, int id) throws Exception;
    }

    private static void runSingle(Arguments a, SingleRunner runner) throws Exception {
        Stats stats = new Stats();
        try (Connection nc = connect(a)) {
            runner.run(nc, nc.jetStream(), stats);
        }
        reportStats(stats, "Total", true, true);
    }

    private static void runShared(Arguments a, ThreadedRunner runner) throws Exception {
        List<Thread> threads = new ArrayList<>();
        List<Stats> statsList = new ArrayList<>();
        try (Connection nc = connect(a)) {
            final JetStream js = nc.jetStream();
            for (int x = 0; x < a.threads; x++) {
                final int id = x + 1;
                final Stats stats = new Stats();
                Thread t = new Thread(() -> {
                    try {
                        runner.run(nc, js, stats, id);
                    } catch (Exception e) {
                        System.out.println("\n Error in thread " + id);
                        e.printStackTrace();
                    }
                });
                statsList.add(stats);
                threads.add(t);
            }
            for (Thread t : threads) {
                t.start();
            }
            for (Thread t : threads) {
                t.join();
            }
        }
        reportStats(statsList);
    }

    private static void runIndividual(Arguments a, ThreadedRunner runner) throws Exception {
        List<Thread> threads = new ArrayList<>();
        List<Stats> statsList = new ArrayList<>();
        for (int x = 0; x < a.threads; x++) {
            final int id = x + 1;
            final Stats stats = new Stats();
            Thread t = new Thread(() -> {
                try (Connection nc = connect(a)) {
                    runner.run(nc, nc.jetStream(), stats, id);
                } catch (Exception e) {
                    System.out.println("\n Error in thread " + id);
                    e.printStackTrace();
                }
            });
            statsList.add(stats);
            threads.add(t);
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        reportStats(statsList);
    }

    // ----------------------------------------------------------------------------------------------------
    // General Utility
    // ----------------------------------------------------------------------------------------------------
    private static String format(Number s) {
        return NumberFormat.getNumberInstance(Locale.getDefault()).format(s);
    }

    private static final String ZEROS = "000000";
    private static String format3(Number s) {
        String f = format(s);
        int at = f.indexOf('.');
        if (at == 0) {
            return f + "." + ZEROS.substring(0, 3);
        }
        return (f + ZEROS).substring(0, at + 3 + 1);
    }

    static class Stats {
        double elapsed = 0;
        double bytes = 0;
        int messageCount = 0;
        long now;

        void start() {
            now = System.nanoTime();
        }

        void stop() {
            elapsed += System.nanoTime() - now;
        }

        void count(long bytes) {
            messageCount++;
            this.bytes += bytes;
        }

        void stopAndCount(long bytes) {
            elapsed += System.nanoTime() - now;
            count(bytes);
        }
    }

    private static final String REPORT_SEP_LINE = "| ---------- | ----------------- | --------------- | ----------------------- | ---------------- |";
    private static void reportStats(Stats stats, String label, boolean header, boolean footer) {
        double elapsed = stats.elapsed / 1e6;
        double messagesPerSecond = stats.elapsed == 0 ? 0 : stats.messageCount * 1e9 / stats.elapsed;
        double bytesPerSecond = 1e9 * (stats.bytes)/(stats.elapsed);
        if (header) {
            System.out.println("\n" + REPORT_SEP_LINE);
            System.out.println("|            |             count |            time |                msgs/sec |        bytes/sec |");
            System.out.println(REPORT_SEP_LINE);
        }
        System.out.printf("| %-10s | %12s msgs | %12s ms | %14s msgs/sec | %12s/sec |\n", label,
                format(stats.messageCount),
                format3(elapsed),
                format3(messagesPerSecond),
                humanBytes(bytesPerSecond));

        if (footer) {
            System.out.println(REPORT_SEP_LINE);
        }
    }

    private static void reportStats(List<Stats> statList) {
        Stats total = new Stats();
        int x = 0;
        for (Stats stats : statList) {
            reportStats(stats, "Thread " + (++x), x == 1, false);
            total.elapsed = Math.max(total.elapsed, stats.elapsed);
            total.messageCount += stats.messageCount;
            total.bytes += stats.bytes;
        }

        System.out.println(REPORT_SEP_LINE);
        reportStats(total, "Total", false, true);
    }

    private static final long HUMAN_BYTES_BASE = 1024;
    private static final String[] HUMAN_BYTES_UNITS = new String[] {"b", "kb", "mb", "gb", "tb", "pb", "eb"};
    public static String humanBytes(double bytes) {
        if (bytes < HUMAN_BYTES_BASE) {
            return String.format("%.2f b", bytes);
        }
        int exp = (int) (Math.log(bytes) / Math.log(HUMAN_BYTES_BASE));
        return String.format("%.2f %s", bytes / Math.pow(HUMAN_BYTES_BASE, exp), HUMAN_BYTES_UNITS[exp]);
    }

    // ----------------------------------------------------------------------------------------------------
    // Arguments
    // ----------------------------------------------------------------------------------------------------
    static class Arguments {
        String server = Options.DEFAULT_URL;
        String action;
        int totalMsgs = 1_000_000;
        int payloadSize = 128;
        int roundSize = 100;
        int batchSize = 10;
        int replicas = 1;
        int threads = 1;
        StorageType storageType = StorageType.Memory;
        AckPolicy ackPolicy = AckPolicy.Explicit;
        int ackFrequency = 1;
        boolean shared = true;
        String stream;
        String subject;
        long jitter = 0;
        int reportFrequency = 1000;
        boolean pullTypeIterate = true;

        private byte[] _payload;
        private byte[] getPayload() {
            if (_payload == null) {
                _payload = new byte[payloadSize];
            }
            return _payload;
        }

        int perThread() {
            return totalMsgs / threads;
        }

        @Override
        public String toString() {
            return "Multi-Tool Run Config:"
                    + "\n  action (-a):              " + action
                    + "\n  server (-s):              " + server
                    + "\n  stream (-t):              " + stream
                    + "\n  storage type (-o):        " + storageType
                    + "\n  replicas (-c):            " + replicas
                    + "\n  subject (-u):             " + subject
                    + "\n  message count (-m):       " + totalMsgs
                    + "\n  threads (-d):             " + threads
                    + "\n  connection strategy (-n): " + (shared ? "shared" : "individual")
                    + "\n  jitter (-j):              " + jitter
                    + "\n  payload size (-p):        " + payloadSize + " bytes"
                    + "\n  round size (-r):          " + roundSize
                    + "\n  ack policy (-kp):         " + ackPolicy
                    + "\n  ack frequency (-kf):      " + ackFrequency
                    + "\n  pull type (-pt):          " + (pullTypeIterate ? "iterate" : "fetch")
                    + "\n  batch size (-b):          " + batchSize
                    + "\n  report frequency (-rf):   " + (reportFrequency == Integer.MAX_VALUE ? "no reporting" : "" + reportFrequency)
                    ;
        }
    }

    private static String normalize(String s) {
        return s.replaceAll("_", "").replaceAll(",", "").replaceAll("\\.", "");
    }

    private static int asNumber(String[] args, int index, int upper, String label) {
        int v = Integer.parseInt(normalize(asString(args, index)));
        if (upper == -2 && v < 1) {
            return Integer.MAX_VALUE;
        }
        if (upper > 0) {
            if (v > upper) {
                error("value for " + label + " cannot exceed " + upper);
            }
        }
        return v;
    }

    private static String asString(String[] args, int index) {
        return args[index].trim();
    }

    private static boolean trueIfNot(String[] args, int index, String notDefault) {
        return !asString(args, index).equalsIgnoreCase(notDefault);
    }

    private static Arguments readArgs(String[] args) {
        if (args == null || args.length == 0) {
            exit();
        }
        Arguments a = new Arguments();

        if (args != null && args.length > 0) {
            for (int x = 0; x < args.length; x++) {
                String arg = args[x].trim();
                switch (arg) {
                    case "-s":
                        a.server = asString(args, ++x);
                        break;
                    case "-a":
                        a.action = asString(args, ++x).toLowerCase();
                        break;
                    case "-t":
                        a.stream = asString(args, ++x);
                        break;
                    case "-u":
                        a.subject = asString(args, ++x);
                        break;
                    case "-m":
                        a.totalMsgs = asNumber(args, ++x, -1, "total messages");
                        break;
                    case "-ps":
                        a.payloadSize = asNumber(args, ++x, 8192, "payload size");
                        break;
                    case "-bs":
                        a.batchSize = asNumber(args, ++x, 256, "batch size");
                        break;
                    case "-rs":
                        a.roundSize = asNumber(args, ++x, 1000, "round size");
                        break;
                    case "-d":
                        a.threads = asNumber(args, ++x, 10, "number of threads");
                        break;
                    case "-c":
                        a.replicas = asNumber(args, ++x, 5, "number of replicas");
                        break;
                    case "-j":
                        a.jitter = asNumber(args, ++x, 10_000, "jitter");
                        break;
                    case "-n":
                        a.shared = trueIfNot(args, ++x, "individual");
                        break;
                    case "-o":
                        a.storageType = trueIfNot(args, ++x, "file") ? StorageType.Memory : StorageType.File;
                        break;
                    case "-kp":
                        a.ackPolicy = AckPolicy.get(asString(args, ++x).toLowerCase());
                        if (a.ackPolicy == null) {
                            a.ackPolicy = AckPolicy.Explicit;
                        }
                        break;
                    case "-kf":
                        a.ackFrequency = asNumber(args, ++x, 256, "ack frequency");
                        break;
                    case "-pt":
                        a.pullTypeIterate = trueIfNot(args, ++x, "fetch");
                        break;
                    case "-rf":
                        a.reportFrequency = asNumber(args, ++x, -2, "report frequency");
                        break;
                    case "":
                        break;
                    default:
                        error("Unknown argument: " + arg);
                        break;
                }
            }
        }

        if (a.action == null || !contains(ALL_ACTIONS, a.action)) {
            error("Valid action required!");
        }

        if (a.stream == null && contains(STREAM_ACTIONS, a.action)) {
            error("Stream actions require stream name!");
        }

        if (a.subject == null && contains(SUBJECT_ACTIONS, a.action)) {
            error("Publish or Subscribe actions require subject name!");
        }

        if (a.threads < 2 && contains(QUEUE_ACTIONS, a.action)) {
            error("Queue subscribing requires multiple threads!");
        }

        if (a.ackPolicy != AckPolicy.Explicit && contains(PULL_ACTIONS, a.action)) {
            error("Consumer in pull mode requires explicit ack policy");
        }

        try {
            new Options.Builder().build().createURIForServer(a.server);
        } catch (URISyntaxException e) {
            error("Invalid server URI: " + a.server);
        }

        System.out.println(a + "\n");

        return a;
    }

    private static void error(String errMsg) {
        System.err.println("\nERROR: " + errMsg);
        exit();
    }

    private static void exit() {
        System.err.println(usageString);
        System.exit(-1);
    }

    private static boolean contains(String list, String action) {
        return Arrays.asList(list.split(" ")).contains(action);
    }

    static final String ALL_ACTIONS = "create delete info pubcore pubsync pubasync subpush subqueue subpull subpullqueue";
    static final String STREAM_ACTIONS = "create delete info";
    static final String SUBJECT_ACTIONS = "pubcore pubsync pubasync subpush subqueue subpull subpullqueue";
    static final String PULL_ACTIONS = "subpull subpullqueue";
    static final String QUEUE_ACTIONS = "subqueue subpullqueue";
    static final String CREATE = "create";
    static final String DELETE = "delete";
    static final String INFO = "info";
    static final String PUB_CORE = "pubcore";
    static final String PUB_SYNC = "pubsync";
    static final String PUB_ASYNC = "pubasync";
    static final String SUB_PUSH = "subpush";
    static final String SUB_QUEUE = "subqueue";
    static final String SUB_PULL = "subpull";
    static final String SUB_PULL_QUEUE = "subpullqueue";
}
