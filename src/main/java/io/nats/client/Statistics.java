/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks various stats received and sent on this connection, including counts for messages and
 * bytes.
 */
public class Statistics implements Cloneable {

    private AtomicLong inMsgs = new AtomicLong();
    private AtomicLong outMsgs = new AtomicLong();
    private AtomicLong inBytes = new AtomicLong();
    private AtomicLong outBytes = new AtomicLong();
    private AtomicLong reconnects = new AtomicLong();
    private AtomicLong flushes = new AtomicLong();

    public Statistics() {
    }

    // deep copy contructor
    Statistics(Statistics obj) {
        this.inMsgs = obj.inMsgs;
        this.inBytes = obj.inBytes;
        this.outBytes = obj.outBytes;
        this.outMsgs = obj.outMsgs;
        this.reconnects = obj.reconnects;
        this.flushes = obj.flushes;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /**
     * Clears the statistics.
     */
    public void clear() {
        this.inBytes.set(0L);
        this.inMsgs.set(0L);
        this.outBytes.set(0L);
        this.outMsgs.set(0L);
        this.flushes.set(0L);
    }

    /**
     * {@inheritDoc}.
     */
    public String toString() {
        return String.format(
                "{in: msgs=%s bytes=%s out: msgs=%s bytes=%s, reconnects: %s flushes: %s}",
                NumberFormat.getNumberInstance(Locale.US).format(inMsgs.get()),
                NumberFormat.getNumberInstance(Locale.US).format(inBytes.get()),
                NumberFormat.getNumberInstance(Locale.US).format(outMsgs.get()),
                NumberFormat.getNumberInstance(Locale.US).format(outBytes.get()),
                NumberFormat.getNumberInstance(Locale.US).format(reconnects.get()),
                NumberFormat.getNumberInstance(Locale.US).format(flushes.get()));
    }

    /**
     * Returns the number of messages that have been received on this Connection.
     *
     * @return the number of messages
     */
    public long getInMsgs() {
        return inMsgs.get();
    }

    /**
     * Increments the number of messages received on this connection.
     */
    long incrementInMsgs() {
        return inMsgs.incrementAndGet();
    }

    /**
     * Returns the number of messages published on this Connection.
     *
     * @return the number of messages
     */
    public long getOutMsgs() {
        return outMsgs.get();
    }

    long incrementOutMsgs() {
        return outMsgs.incrementAndGet();
    }

    /**
     * Returns the number of bytes received on this Connection.
     *
     * @return the number of bytes
     */
    public long getInBytes() {
        return inBytes.get();
    }

    /*
     * Increments the number of bytes received.
     */
    long incrementInBytes(long amount) {
        return inBytes.addAndGet(amount);
    }

    /**
     * Returns the number of bytes that have been output on this Connection.
     *
     * @return the number of bytes
     */
    public long getOutBytes() {
        return outBytes.get();
    }

    /*
     * Increments the number of bytes output
     */
    long incrementOutBytes(long delta) {
        return outBytes.addAndGet(delta);
    }

    /**
     * Returns the number of times this Connection has reconnected.
     *
     * @return the number of reconnects
     */
    public long getReconnects() {
        return reconnects.get();
    }

    long incrementReconnects() {
        return reconnects.incrementAndGet();
    }

    /**
     * Returns the number of times this Connection's underlying TCP connection has been flushed.
     *
     * @return the number of flushes
     */
    long getFlushes() {
        return flushes.get();
    }

    long incrementFlushes() {
        return flushes.incrementAndGet();
    }

}

