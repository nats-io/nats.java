/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.util.concurrent.atomic.AtomicLong;

// Tracks various stats received and sent on this connection,
// including counts for messages and bytes.
public class Statistics implements Cloneable {

	private AtomicLong inMsgs = new AtomicLong();
	private AtomicLong outMsgs = new AtomicLong();
	private AtomicLong inBytes = new AtomicLong();
	private AtomicLong outBytes = new AtomicLong();
	private AtomicLong reconnects = new AtomicLong();
	private AtomicLong flushes = new AtomicLong();
	
	Statistics () {
		
	}
	
    // deep copy contructor
    Statistics(Statistics obj)
    {
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

	public void clear() {
		this.inBytes.set(0L);
		this.inMsgs.set(0L);
		this.outBytes.set(0L);
		this.outMsgs.set(0L);
		this.flushes.set(0L);
	}

	public String toString() {
		return String.format("{in: msgs=%d, bytes=%d, out: msgs=%d, bytes=%d, reconnects: %d, flushes: %d}", 
				inMsgs.get(), inBytes.get(), outMsgs.get(), outBytes.get(), reconnects.get(), flushes.get());
	}
	/**
	 * @return the number of messages that have been 
	 * received on this Connection.
	 */
	public long getInMsgs() {
		return inMsgs.get();
	}

	/**
	 * Increments the number of messages received on 
	 * this connection. 
	 */
	long incrementInMsgs() {
		return inMsgs.incrementAndGet();
	}

	/**
	 * @return the number of messages published on 
	 * this Connection.
	 */
	public long getOutMsgs() {
		return outMsgs.get();
	}

	long incrementOutMsgs() {
		return outMsgs.incrementAndGet();
	}

	/**
	 * @return the number of bytes received on this
	 * Connection.
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
	 * @return the number of bytes that have been output
	 * on this Connection.
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
	 * @return the number of times this Connection has 
	 * reconnected.
	 */
	public long getReconnects() {
		return reconnects.get();
	}
	
	long incrementReconnects() {
		return reconnects.incrementAndGet();
	}
	
	/**
	 * @return the number of times this Connection has 
	 * reconnected.
	 */
	long getFlushes() {
		return flushes.get();
	}
	
	long incrementFlushes() {
		return flushes.incrementAndGet();
	}

}

