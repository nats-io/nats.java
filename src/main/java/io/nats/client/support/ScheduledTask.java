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

package io.nats.client.support;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! *
 * WARNING: THIS CLASS IS PUBLIC BUT ITS API IS NOT GUARANTEED TO *
 * BE BACKWARD COMPATIBLE AS IT IS INTENDED AS AN INTERNAL CLASS  *
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! *
 */
public class ScheduledTask implements Runnable {
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private final String id;
    private final Runnable runnable;
    protected final AtomicReference<ScheduledFuture<?>> scheduledFutureRef;

    protected final AtomicBoolean notShutdown;
    protected final AtomicBoolean executing;
    protected final long initialDelayNanos;
    protected final long periodNanos;

    public ScheduledTask(ScheduledExecutorService ses, long initialAndPeriodMillis, Runnable runnable) {
        this(null, ses, initialAndPeriodMillis, initialAndPeriodMillis, TimeUnit.MILLISECONDS, runnable);
    }

    public ScheduledTask(String id, ScheduledExecutorService ses, long initialAndPeriodMillis, Runnable runnable) {
        this(id, ses, initialAndPeriodMillis, initialAndPeriodMillis, TimeUnit.MILLISECONDS, runnable);
    }

    public ScheduledTask(ScheduledExecutorService ses, long initialAndPeriod, TimeUnit unit, Runnable runnable) {
        this(null, ses, initialAndPeriod, initialAndPeriod, unit, runnable);
    }

    public ScheduledTask(String id, ScheduledExecutorService ses, long initialAndPeriod, TimeUnit unit, Runnable runnable) {
        this(id, ses, initialAndPeriod, initialAndPeriod, unit, runnable);
    }

    public ScheduledTask(ScheduledExecutorService ses, long initialDelay, long period, TimeUnit unit, Runnable runnable) {
        this(null, ses, initialDelay, period, unit, runnable);
    }

    public ScheduledTask(String id, ScheduledExecutorService ses, long initialDelay, long period, TimeUnit unit, Runnable runnable) {
        this.id = id == null || id.isEmpty() ? "st-" + ID_GENERATOR.getAndIncrement() : id;
        this.runnable = runnable;
        notShutdown = new AtomicBoolean(true);
        executing = new AtomicBoolean(false);
        this.initialDelayNanos = unit.toNanos(initialDelay);
        this.periodNanos = unit.toNanos(period);
        scheduledFutureRef = new AtomicReference<>(
            ses.scheduleAtFixedRate(this, initialDelay, period, unit));
    }

    public long getInitialDelayNanos() {
        return initialDelayNanos;
    }

    public long getPeriodNanos() {
        return periodNanos;
    }

    @Override
    public void run() {
        try {
            if (notShutdown.get()) {
                executing.set(true);
                runnable.run();
            }
        }
        finally {
            executing.set(false);
        }
    }

    public boolean isShutdown() {
        return !notShutdown.get();
    }

    public boolean isExecuting() {
        return executing.get();
    }

    public boolean isDone() {
        ScheduledFuture<?> f = scheduledFutureRef.get();
        return f == null || f.isDone();
    }

    public String getId() {
        return id;
    }

    public void shutdown() {
        try {
            notShutdown.set(false);
            ScheduledFuture<?> f = scheduledFutureRef.get();
            if (f != null) {
                scheduledFutureRef.set(null); // just releasing resources.
                if (!f.isDone()) {
                    f.cancel(false);
                }
            }
        }
        catch (Exception ignore) {
            // don't want this to be passed along
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(id);
        if (notShutdown.get()) {
            sb.append(" [live");
        }
        else {
            sb.append(" [shutdown");
        }
        sb.append(isDone() ? "/done" : "/!done");
        sb.append(executing.get() ? "/executing" : "/!executing");
        sb.append("]");
        return sb.toString();
    }
}
