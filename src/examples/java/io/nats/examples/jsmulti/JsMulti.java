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
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PublishAck;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static io.nats.examples.ExampleUtils.*;
import static io.nats.examples.jsmulti.Constants.*;

public class JsMulti {

    public static void main(String[] args) throws Exception {
        run(new Arguments(args));
    }

    public static List<Stats> run(Arguments a) {
        try {
            if (a.threads > 1) {
                if (a.connShared) {
                    ThreadedRunner tr;
                    switch (a.action) {
                        case PUB_SYNC:
                            tr = (nc, js, stats, id) -> pubSync(a, js, stats, "pubSyncShared " + id);
                            break;
                        case PUB_ASYNC:
                            tr = (nc, js, stats, id) -> pubAsync(a, js, stats, "pubAsyncShared " + id);
                            break;
                        case PUB_CORE:
                            tr = (nc, js, stats, id) -> pubCore(a, nc, stats, "pubCoreShared " + id);
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
                            return null;
                    }
                    return runShared(a, tr);
                }
                else {
                    ThreadedRunner tr;
                    switch (a.action) {
                        case PUB_SYNC:
                            tr = (nc, js, stats, id) -> pubSync(a, js, stats, "pubSyncIndividual " + id);
                            break;
                        case PUB_ASYNC:
                            tr = (nc, js, stats, id) -> pubAsync(a, js, stats, "pubAsyncIndividual " + id);
                            break;
                        case PUB_CORE:
                            tr = (nc, js, stats, id) -> pubCore(a, nc, stats, "pubCoreIndividual " + id);
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
                            return null;
                    }
                    return runIndividual(a, tr);
                }
            }
            else {
                SingleRunner sr;
                switch (a.action) {
                    case PUB_SYNC:
                        sr = (nc, js, stats) -> pubSync(a, js, stats, "pubSync");
                        break;
                    case PUB_ASYNC:
                        sr = (nc, js, stats) -> pubAsync(a, js, stats, "pubAsync");
                        break;
                    case PUB_CORE:
                        sr = (nc, js, stats) -> pubCore(a, nc, stats, "pubCore");
                        break;
                    case SUB_PUSH:
                        sr = (nc, js, stats) -> subPush(a, js, stats, false, "subPush");
                        break;
                    case SUB_PULL:
                        sr = (nc, js, stats) -> subPull(a, js, stats, 0, "subPull");
                        break;
                    default:
                        return null;
                }
                return runSingle(a, sr);
            }
        }
        catch (Exception e) {
            //noinspection ThrowablePrintedToSystemOut
            System.out.println(e);
            e.printStackTrace();
            System.exit(-1);
            return null;
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Implementation
    // ----------------------------------------------------------------------------------------------------
    private static void pubSync(Arguments a, final JetStream js, Stats stats, String label) throws Exception {
        _pub(a, stats, label, (p) -> js.publish(a.subject, p) );
    }

    private static void pubCore(Arguments a, final Connection nc, Stats stats, String label) throws Exception {
        _pub(a, stats, label, (p) -> nc.publish(a.subject, p));
    }

    interface Publisher {
        void publish(byte[] payload) throws Exception;
    }

    private static void _pub(Arguments a, Stats stats, String label, Publisher p) throws Exception {
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

    private static void pubAsync(Arguments a, JetStream js, Stats stats, String label) {
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
            futures.add(js.publishAsync(a.subject, payload));
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

    private static List<Stats> runSingle(Arguments a, SingleRunner runner) throws Exception {
        Stats stats = new Stats();
        try (Connection nc = connect(a)) {
            runner.run(nc, nc.jetStream(), stats);
        }
        Stats.report(stats);
        return Collections.singletonList(stats);
    }

    private static List<Stats>  runShared(Arguments a, ThreadedRunner runner) throws Exception {
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
        Stats.report(statsList);
        return statsList;
    }

    private static List<Stats> runIndividual(Arguments a, ThreadedRunner runner) throws Exception {
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
        Stats.report(statsList);
        return statsList;
    }
}
