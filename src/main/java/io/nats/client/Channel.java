/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.util.Collection;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Channel<T> {

	final static Logger logger = LoggerFactory.getLogger(Channel.class);
	/**
	 * This channel class is really a blocking queue, is named the way 
	 * it is so the code more closely reads with Go.
	 */
	LinkedBlockingQueue<T> q;
	T defaultVal = null;

	Channel() {
		q = new LinkedBlockingQueue<T>();
	}

	Channel(LinkedBlockingQueue<T> queue) {
		q = queue;
	}

	Channel(int capacity) {
		if (capacity <= 0)
			q = new LinkedBlockingQueue<T>();
		else
			q = new LinkedBlockingQueue<T>(capacity);
	}

	public Channel(Collection<T> c) {
		q = new LinkedBlockingQueue<T>(c);
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
		return(get(timeout, TimeUnit.MILLISECONDS));
	}
	
	T get(long timeout, TimeUnit unit) throws TimeoutException {
		T item = defaultVal;

		try {
			if (timeout < 0)
				item = q.take();
			else {
				item = q.poll(timeout, unit);
				if (item==null) {
					throw new TimeoutException("Channel timed out waiting for items");
				}
			}
		} catch (InterruptedException e) {
		}
		return item;
	}
	
	T poll() {
		return q.poll();
	}

	// Will throw NullPointerException if you try to insert a null item
	boolean add(T item)
	{
		// offer(T e) is used here simply to eliminate exceptions. add returns false only
		// if adding the item would have exceeded the capacity of a bounded queue.
			return q.offer(item);
	}

	public void close()
	{
//		logger.trace("Channel.close(), clearing queue");
		q.clear();
	}

	int getCount()
	{
		return q.size();
	}

}