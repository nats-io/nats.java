/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NATSThread.
 * <p/>
 * Custom thread base class
 *
 * @author Brian Goetz and Tim Peierls
 */
class NATSThread extends Thread {
    public static final String DEFAULT_NAME = "NATSThread";
    private static volatile boolean debugLifecycle = false;
    private static final AtomicInteger created = new AtomicInteger();
    private static final AtomicInteger alive = new AtomicInteger();
    private CountDownLatch startSignal = null;
    private CountDownLatch doneSignal = null;
    private final Logger logger = LoggerFactory.getLogger(NATSThread.class);

    public NATSThread(Runnable r) {
        this(r, DEFAULT_NAME);
    }

    public NATSThread(Runnable runnable, String name) {
        super(runnable, name + "-" + created.incrementAndGet());
        setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                logger.debug("UNCAUGHT in thread {}", t.getName(), e);
            }
        });
    }

    public NATSThread(Runnable r, String poolName, CountDownLatch startSignal,
            CountDownLatch doneSignal) {
        super(r, poolName);
        this.startSignal = startSignal;
        this.doneSignal = doneSignal;
    }

    public void run() {
        logger.trace("In RUN");
        // Copy debug flag to ensure consistent value throughout.
        boolean debug = debugLifecycle;
        if (debug) {
            logger.debug("Created {}", getName());
        }
        try {
            if (startSignal != null) {
                startSignal.await();
            }
            alive.incrementAndGet();
            super.run();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            logger.trace("Interrupted: ", e);
        } finally {
            if (this.doneSignal != null) {
                this.doneSignal.countDown();
            }
            alive.decrementAndGet();
            if (debug) {
                logger.debug("Exiting {}" + getName());
            }
        }
    }

    public static int getThreadsCreated() {
        return created.get();
    }

    public static int getThreadsAlive() {
        return alive.get();
    }

    public static boolean getDebug() {
        return debugLifecycle;
    }

    public static void setDebug(boolean b) {
        debugLifecycle = b;
    }
}
