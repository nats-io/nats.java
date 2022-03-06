 // Copyright 2021-2022 The NATS Authors
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
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static io.nats.examples.ExampleUtils.*;
import static io.nats.examples.jsmulti.Constants.*;

public class JsMulti {

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static List<Stats> run(String[] args) throws Exception {
        return run(new Arguments(args));
    }

    public static List<Stats> run(Arguments a) throws Exception {
        Runner aRunner = getRunner(a, a.action);
        Runner lRunner = a.latencyAction == null ? null : getRunner(a, a.latencyAction);
        if (a.connShared) {
            return runShared(a, aRunner, lRunner);
        }
        return runIndividual(a, aRunner, lRunner);
    }

    private static Runner getRunner(Arguments a, String action) {
        String label = a.connShared ? "Shared" : "Individual";
        try {
            switch (action) {
                case PUB_SYNC:
                    return (nc, js, stats, id) -> pubSync(a, js, stats, "pubSync" + label + " " + id);
                case PUB_ASYNC:
                    return (nc, js, stats, id) -> pubAsync(a, js, stats, "pubAsync" + label + " " + id);
                case PUB_CORE:
                    return (nc, js, stats, id) -> pubCore(a, nc, stats, "pubCore" + label + " " + id);
                case SUB_PUSH:
                    return (nc, js, stats, id) -> subPush(a, js, stats, false, "subPush" + label + " " + id);
                case SUB_PULL:
                    return (nc, js, stats, id) -> subPull(a, js, stats, id, "subPullS" + label + " " + id);
                case SUB_QUEUE:
                    if (a.threads > 1) {
                        return (nc, js, stats, id) -> subPush(a, js, stats, true, "subPush" + label + "Queue " + id);
                    }
                    break;
                case SUB_PULL_QUEUE:
                    if (a.threads > 1) {
                        return (nc, js, stats, id) -> subPull(a, js, stats, 0, "subPull" + label + "Queue " + id);
                    }
                    break;
            }
            System.out.println("Invalid Action");
            System.exit(-1);
        }
        catch (Exception e) {
            //noinspection ThrowablePrintedToSystemOut
            System.out.println(e);
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    // ----------------------------------------------------------------------------------------------------
    // Implementation
    // ----------------------------------------------------------------------------------------------------
    interface Publisher<T> {
        T publish(byte[] payload) throws Exception;
    }

    private static NatsMessage buildLatencyMessage(Arguments a, byte[] p) {
        return NatsMessage.builder()
            .subject(a.subject)
            .data(p)
            .headers(new Headers().put(HDR_PUB_TIME, "" + System.currentTimeMillis()))
            .build();
    }

    private static void pubSync(Arguments a, final JetStream js, Stats stats, String label) throws Exception {
        if (a.latencyTracking) {
            _pub(a, stats, label, (p) -> js.publish(buildLatencyMessage(a, p)));
        }
        else {
            _pub(a, stats, label, (p) -> js.publish(a.subject, p));
        }
    }

    private static void pubCore(Arguments a, final Connection nc, Stats stats, String label) throws Exception {
        if (a.latencyTracking) {
            _pub(a, stats, label, (p) -> {
                nc.publish(buildLatencyMessage(a, p));
                return null;
            });
        }
        else {
            _pub(a, stats, label, (p) -> {
                nc.publish(a.subject, p);
                return null;
            });
        }
    }

    private static void _pub(Arguments a, Stats stats, String label, Publisher<PublishAck> p) throws Exception {
        for (int x = 1; x <= a.perThread(); x++) {
            jitter(a);
            byte[] payload = a.getPayload();
            stats.start();
            p.publish(payload);
            stats.stopAndCount(a.payloadSize);
            reportMaybe(a, x, label, "completed publishing");
        }
        System.out.println(label + " completed publishing");
    }

    private static void pubAsync(final Arguments a, final JetStream js, Stats stats, String label) throws Exception {
        Publisher<CompletableFuture<PublishAck>> publisher;
        if (a.latencyAction == null) {
            publisher = (p) -> js.publishAsync(a.subject, p);
        }
        else {
            publisher = (p) -> js.publishAsync(buildLatencyMessage(a, p));
        }

        List<CompletableFuture<PublishAck>> futures = new ArrayList<>();
        int r = 0;
        for (int x = 1; x <= a.perThread(); x++) {
            if (++r >= a.roundSize) {
                processFutures(futures, stats);
                r = 0;
            }
            jitter(a);
            byte[] payload = a.getPayload();
            stats.start();
            futures.add(publisher.publish(payload));
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
        PushSubscribeOptions pso = PushSubscribeOptions.builder()
                .configuration(cc)
                .durable(q ? a.queueDurable : null)
                .build();
        JetStreamSubscription sub;
        if (q) {
            // if we don't do this, multiple threads will try to make the same consumer because
            // when they start, the consumer does not exist. So force them do it one at a time.
            synchronized (a.queueName) {
                sub = js.subscribe(a.subject, a.queueName, pso);
            }
        }
        else {
            sub = js.subscribe(a.subject, pso);
        }
        int misses = 0;
        int x = 0;
        List<Message> ackList = new ArrayList<>();
        while (x < a.perThread()) {
            jitter(a);
            stats.start();
            Message m = sub.nextMessage(Duration.ofSeconds(1));
            if (m == null) {
                if (++misses > MISS_THRESHOLD) {
                    break;
                }
                continue;
            }
            stats.stopAndCount(m);
            ackMaybe(a, stats, ackList, m);
            reportMaybe(a, ++x, label, "messages read");
        }
        ackEm(a, stats, ackList, 1);
        System.out.println(label + " finished messages read " + Stats.format(x));
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
                case All:
                    ackList.add(m);
                    break;
            }
            ackEm(a, stats, ackList, a.ackFrequency);
        }
    }

    private static void ackEm(Arguments a, Stats stats, List<Message> ackList, int thresh) {
        if (ackList.size() >= thresh) {
            stats.start();
            switch (a.ackPolicy) {
                case Explicit:
                    for (Message m : ackList) {
                        m.ack();
                    }
                    break;
                case All:
                    ackList.get(ackList.size() - 1).ack();
                    break;
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
            subPullIterate(a, stats, label, sub);
        }
        else {
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
        ackEm(a, stats, ackList, 1);
        System.out.println(label + " finished messages read " + Stats.format(x));
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
        ackEm(a, stats, ackList, 1);
        System.out.println(label + " finished messages read " + Stats.format(x));
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

    private static Connection connect(Arguments a) throws Exception {
        Options options;
        if (a.optionsFactoryClassName == null) {
            options = createExampleOptions(a.server, false, new ErrorListener() {});
        }
        else {
            Class<?> c = Class.forName(a.optionsFactoryClassName);
            Constructor<?> cons = c.getConstructor();
            OptionsFactory of = (OptionsFactory)cons.newInstance();
            options = of.getOptions(a);
        }
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
    interface Runner {
        void run(Connection nc, JetStream js, Stats stats, int id) throws Exception;
    }

    private static List<Stats> runShared(Arguments a, Runner runner, Runner lrunner) throws Exception {
        List<Stats> statsList = new ArrayList<>();
        try (Connection nc = connect(a)) {
            final JetStream js = nc.jetStream();
            List<Thread> lthreads = new ArrayList<>();
            if (lrunner != null) {
                _runShared(a, lrunner, nc, js, lthreads, statsList, a.latencyAction);
                for (Thread t : lthreads) { t.start(); }
                sleep(500);
            }
            List<Thread> threads = new ArrayList<>();
            _runShared(a, runner, nc, js, threads, lrunner == null ? statsList : null, a.action);
            for (Thread t : threads) { t.start(); }
            for (Thread t : threads) { t.join(); }
            for (Thread t : lthreads) { t.join(); }
        }

        Stats.report(statsList);
        return statsList;
    }

    private static void _runShared(Arguments a, Runner runner, Connection nc, JetStream js, List<Thread> threads, List<Stats> statsList, String hlabel) {
        for (int x = 0; x < a.threads; x++) {
            final int id = x + 1;
            final Stats stats = new Stats(hlabel);
            Thread t = new Thread(() -> {
                try {
                    runner.run(nc, js, stats, id);
                } catch (Exception e) {
                    System.out.println("\n Error in thread " + id);
                    e.printStackTrace();
                }
            });
            if (statsList != null) {
                statsList.add(stats);
            }
            threads.add(t);
        }
    }

    private static List<Stats> runIndividual(Arguments a, Runner runner, Runner lrunner) throws Exception {
        List<Stats> statsList = new ArrayList<>();

        List<Thread> lthreads = new ArrayList<>();
        if (lrunner != null) {
            _runIndividual(a, lrunner, lthreads, statsList, a.latencyAction);
            for (Thread t : lthreads) { t.start(); }
            sleep(500);
        }

        List<Thread> threads = new ArrayList<>();
        _runIndividual(a, runner, threads, lrunner == null ? statsList : null, a.action);

        for (Thread t : threads) {t.start(); }
        for (Thread t : threads) { t.join(); }
        for (Thread t : lthreads) { t.join(); }

        Stats.report(statsList);
        return statsList;
    }

    private static void _runIndividual(Arguments a, Runner runner, List<Thread> threads, List<Stats> statsList, String hlabel) {
        for (int x = 0; x < a.threads; x++) {
            final int id = x + 1;
            final Stats stats = new Stats(hlabel);
            Thread t = new Thread(() -> {
                try (Connection nc = connect(a)) {
                    runner.run(nc, nc.jetStream(), stats, id);
                } catch (Exception e) {
                    System.out.println("\n Error in thread " + id);
                    e.printStackTrace();
                }
            });

            if (statsList != null) {
                statsList.add(stats);
            }
            threads.add(t);
        }
    }
}
