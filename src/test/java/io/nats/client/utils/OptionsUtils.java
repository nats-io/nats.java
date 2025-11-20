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

import io.nats.client.ErrorListener;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import org.jspecify.annotations.NonNull;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.NatsRunnerUtils.getNatsLocalhostUri;

/**
 * ----------------------------------------------------------------------------------------------------
 * THIS IS THE PREFERRED WAY TO BUILD ANY OPTIONS FOR TESTING
 * ----------------------------------------------------------------------------------------------------
 */
public abstract class OptionsUtils {

    private static ExecutorService ES;
    private static ScheduledThreadPoolExecutor SCHD;
    private static ThreadFactory CB;
    private static ThreadFactory CN;

    public static ErrorListener NOOP_EL = new ErrorListener() {};

    public static Options.Builder optionsBuilder(ErrorListener el) {
        return optionsBuilder().errorListener(el);
    }

    public static Options.Builder optionsBuilder(NatsTestServer ts) {
        return optionsBuilder().server(ts.getNatsLocalhostUri());
    }

    public static Options.Builder optionsBuilder(int port) {
        return optionsBuilder().server(getNatsLocalhostUri(port));
    }

    public static Options.Builder optionsBuilder(String... servers) {
        return optionsBuilder().servers(servers);
    }

    public static Options options(String... servers) {
        return optionsBuilder().servers(servers).build();
    }

    public static Options.Builder optionsBuilder() {
        if (ES == null) {
            ES = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                500L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new TestThreadFactory("ES"));
        }

        if (SCHD == null) {
            SCHD = new ScheduledThreadPoolExecutor(3, new TestThreadFactory("SCHD"));
            SCHD.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            SCHD.setRemoveOnCancelPolicy(true);
        }

        if (CB == null) {
            CB = new TestThreadFactory("CB");
        }

        if (CN == null) {
            CN = new TestThreadFactory("CN");
        }

        return Options.builder()
            .executor(ES)
            .scheduledExecutor(SCHD)
            .callbackThreadFactory(CB)
            .connectThreadFactory(CN)
            .errorListener(NOOP_EL);
    }

    static class TestThreadFactory implements ThreadFactory {
        final String name;
        final AtomicInteger threadNo;

        public TestThreadFactory(String name) {
            this.name = name;
            this.threadNo = new AtomicInteger(0);
        }

        public Thread newThread(@NonNull Runnable r) {
            String threadName = "test." + name + "." + threadNo.incrementAndGet();
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
