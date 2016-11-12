/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

/**
 * NatsThreadFactory <p/> Custom thread factory.
 *
 * @author Brian Goetz and Tim Peierls
 */
class NatsThreadFactory implements ThreadFactory {
    private final String poolName;
    private final CountDownLatch startSignal;
    private final CountDownLatch doneSignal;

    public NatsThreadFactory(String poolName) {
        this(poolName, null, null);
    }

    public NatsThreadFactory(String poolName, CountDownLatch startSignal,
                             CountDownLatch doneSignal) {
        this.poolName = poolName;
        this.startSignal = startSignal;
        this.doneSignal = doneSignal;
    }

    public Thread newThread(Runnable runnable, CountDownLatch startSignal,
                            CountDownLatch doneSignal) {
        return new NatsThread(runnable, poolName, startSignal, doneSignal);
    }

    @Override
    public Thread newThread(Runnable runnable) {
        return newThread(runnable, null, null);
    }

}
