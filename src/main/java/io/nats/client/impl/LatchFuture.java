package io.nats.client.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Future implementation based on CountDownLatch.
 * @param <T>
 */
public class LatchFuture<T> extends CountDownLatch implements Future<T> {

    private boolean cancelled;
    private T result;

    /**
     * New future
     */
    public LatchFuture() {
        super(1);
    }

    /**
     * Already completed future
     * @param result
     */
    public LatchFuture(T result) {
        super(0);
        this.result = result;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        cancelled = true;
        countDown();
        return cancelled;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public boolean isDone() {
        return getCount() == 0;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        await();
        return result;
    }

    @Override
    public T get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        await(l, timeUnit);
        return result;
    }

    /**
     * Stores result and counts down the latch
     * @param result
     */
    public void complete(T result) {
        this.result = result;
        countDown();
    }

    /**
     * @param valueIfAbsent  return this if result is unavailable
     * @return the result if available, otherwise returns valueIfAbsent
     */
    public T getNow(T valueIfAbsent) {
        return result != null ? result : valueIfAbsent;
    }
}
