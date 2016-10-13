/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@code Channel} is a utility wrapper for {@link LinkedBlockingQueue}. Although its visibility is
 * currently {@code public}, it is not considered an official part of the NATS API and may be
 * deprecated at any time.
 */
class Channel<T> {

    static final Logger logger = LoggerFactory.getLogger(Channel.class);
    /**
     * This channel class is really a blocking queue, is named the way it is so the code more
     * closely reads with Go.
     */
    LinkedBlockingQueue<T> q;
    T defaultVal = null;
    boolean closed = false;

    Channel() {
        this(-1);
    }

    Channel(LinkedBlockingQueue<T> queue) {
        q = queue;
    }

    Channel(int capacity) {
        if (capacity <= 0) {
            q = new LinkedBlockingQueue<T>();
        } else {
            q = new LinkedBlockingQueue<T>(capacity);
        }
    }

    Channel(Collection<T> collection) {
        q = new LinkedBlockingQueue<T>(collection);
    }

    T get() {
        T result = defaultVal;
        try {
            result = get(-1, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Can't happen
            throw new Error("Unexpected error: " + e.getMessage());
        }
        return result;
    }

    synchronized T get(long timeout) throws TimeoutException {
        return (get(timeout, TimeUnit.MILLISECONDS));
    }

    T get(long timeout, TimeUnit unit) throws TimeoutException {
        T item = defaultVal;

        try {
            if (timeout < 0) {
                item = q.take();
            } else {
                item = q.poll(timeout, unit);
                if (item == null) {
                    throw new TimeoutException("Channel timed out waiting for items");
                }
            }
        } catch (InterruptedException e) {
        }
        return item;
    }

    // T getNew(long timeout, TimeUnit unit) throws TimeoutException {
    // // System.err.printf("get called with timeout=%d, unit=%s\n", timeout, unit);
    // T item = null;
    // ExecutorService executor = Executors.newSingleThreadExecutor();
    // Future<T> future = executor.submit(new Callable<T>() {
    // public T call() throws Exception {
    // // do operations you want
    // T theItem = null;
    // while (theItem == null) {
    // if (isClosed()) {
    // break;
    // }
    // theItem = q.poll();
    // }
    // return theItem;
    // }
    // });
    // try {
    // if (timeout < 0) {
    // item = future.get();
    // } else {
    // item = future.get(timeout, unit);
    // }
    // } catch (TimeoutException e) {
    // future.cancel(true);
    // throw new TimeoutException("Channel timed out waiting for items");
    // // throw e;
    // } catch (InterruptedException e) {
    // e.printStackTrace();
    // } catch (ExecutionException e) {
    // e.printStackTrace();
    // }
    // executor.shutdownNow();
    // return item;
    // }


    T poll() {
        return q.poll();
    }

    // Will throw NullPointerException if you try to insert a null item
    boolean add(T item) {
        // offer(T e) is used here simply to eliminate exceptions. add returns false only
        // if adding the item would have exceeded the capacity of a bounded queue.
        if (isClosed()) {
            return false;
        }
        return q.offer(item);
    }

    boolean add(T item, long timeout, TimeUnit unit) throws InterruptedException {
        if (isClosed()) {
            return false;
        }
        return q.offer(item, timeout, unit);
    }

    void close() {
        // logger.trace("Channel.close(), clearing queue");
        closed = true;
        q.clear();
    }

    boolean isClosed() {
        return closed;
    }

    int getCount() {
        return q.size();
    }
}
