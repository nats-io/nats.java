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