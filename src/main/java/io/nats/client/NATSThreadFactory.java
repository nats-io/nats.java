/*******************************************************************************
 * Copyright (c) 2012, 2015 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.util.concurrent.ThreadFactory;

/**
 * NATSThreadFactory
 * <p/>
 * Custom thread factory
 *
 * @author Brian Goetz and Tim Peierls
 */
class NATSThreadFactory implements ThreadFactory {
    private final String poolName;

    public NATSThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    public Thread newThread(Runnable runnable) {
        return new NATSThread(runnable, poolName);
    }
}