/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NatsThread. <p/> Custom thread base class
 *
 * @author Brian Goetz and Tim Peierls
 */
class NatsThread extends Thread {
    public static final String DEFAULT_NAME = "NatsThread";
    private static final AtomicInteger created = new AtomicInteger();
    private static final AtomicInteger alive = new AtomicInteger();
    private CountDownLatch startSignal = null;
    private CountDownLatch doneSignal = null;

    public NatsThread(Runnable r) {
        this(r, DEFAULT_NAME);
    }

    public NatsThread(Runnable runnable, String name) {
        super(runnable, name + "-" + created.incrementAndGet());
        setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
               e.printStackTrace();
            }
        });
    }

    public NatsThread(Runnable r, String poolName, CountDownLatch startSignal,
                      CountDownLatch doneSignal) {
        super(r, poolName);
        this.startSignal = startSignal;
        this.doneSignal = doneSignal;
    }

    public void run() {
        try {
            if (startSignal != null) {
                startSignal.await();
            }
            alive.incrementAndGet();
            super.run();
        } catch (InterruptedException e) {
        } finally {
            if (this.doneSignal != null) {
                this.doneSignal.countDown();
            }
            alive.decrementAndGet();
        }
    }

    public static int getThreadsCreated() {
        return created.get();
    }

    public static int getThreadsAlive() {
        return alive.get();
    }
}
