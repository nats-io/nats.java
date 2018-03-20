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
