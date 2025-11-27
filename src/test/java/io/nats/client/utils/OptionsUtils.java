// Copyright 2025 The NATS Authors
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

package io.nats.client.utils;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import org.jspecify.annotations.NonNull;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ----------------------------------------------------------------------------------------------------
 * THIS IS THE PREFERRED WAY TO BUILD ANY OPTIONS FOR TESTING
 * ----------------------------------------------------------------------------------------------------
 */
public abstract class OptionsUtils {

    private static ExecutorService EX;
    private static ScheduledThreadPoolExecutor SC;
    private static ExecutorService CB;
    private static ExecutorService CN;

    public static ErrorListener NOOP_EL = new ErrorListener() {};

    public static Options.Builder optionsBuilder(ErrorListener el) {
        return optionsBuilder().errorListener(el);
    }

    public static Options.Builder optionsBuilder(NatsTestServer ts) {
        return optionsBuilder().server(ts.getLocalhostUri());
    }

    public static Options.Builder optionsBuilder(NatsTestServer ts, String schema) {
        return optionsBuilder().server(ts.getLocalhostUri(schema));
    }

    public static Options.Builder optionsBuilder(int port) {
        return optionsBuilder().server(NatsTestServer.getLocalhostUri(port));
    }

    public static Options.Builder optionsBuilder(String... servers) {
        return optionsBuilder().servers(servers);
    }

    public static Options.Builder optionsBuilder(Connection nc) {
        //noinspection DataFlowIssue
        return optionsBuilder().server(nc.getConnectedUrl());
    }

    public static Options options(int port) {
        return optionsBuilder(port).build();
    }

    public static Options options(NatsTestServer ts) {
        return optionsBuilder(ts).build();
    }

    public static Options options(String... servers) {
        return optionsBuilder().servers(servers).build();
    }

    public static Options.Builder optionsBuilder() {
        if (EX == null) {
            EX = new ThreadPoolExecutor(4, Integer.MAX_VALUE, 30, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new TestThreadFactory("ex"));
        }

        if (SC == null) {
            SC = new ScheduledThreadPoolExecutor(3, new TestThreadFactory("sc"));
            SC.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            SC.setRemoveOnCancelPolicy(true);
        }

        if (CB == null) {
            CB = new ThreadPoolExecutor(2, Integer.MAX_VALUE, 30, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new TestThreadFactory("cb"));
        }

        if (CN == null) {
            CN = new ThreadPoolExecutor(4, Integer.MAX_VALUE, 30, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new TestThreadFactory("cn"));
        }

        return Options.builder()
            .connectionTimeout(Duration.ofSeconds(4))
            .executor(EX)
            .scheduledExecutor(SC)
            .callbackExecutor(CB)
            .connectExecutor(CN)
            .errorListener(NOOP_EL);
    }

    static class TestThreadFactory implements ThreadFactory {
        final String name;
        final AtomicInteger threadNumber;

        public TestThreadFactory(String name) {
            this.name = name;
            this.threadNumber = new AtomicInteger(1);
        }

        public Thread newThread(@NonNull Runnable r) {
            String threadName = "test." + name + "." + threadNumber.incrementAndGet();
            Thread t = new Thread(r, threadName);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
