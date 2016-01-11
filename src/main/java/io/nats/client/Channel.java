/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
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
	 * This channel class really a blocking queue, is named the way it is so the
	 * code more closely reads with GO.
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
		q = new LinkedBlockingQueue<T>(capacity);
	}

	public Channel(Collection<T> c) {
		q = new LinkedBlockingQueue<T>(c);
	}

	T get() throws TimeoutException {
		return get(-1);
	}
	
	T get(long timeout, TimeUnit unit) throws TimeoutException {
		T item = defaultVal;

		try {
			if (timeout < 0)
				item = q.take();
			else
				item = q.poll(timeout, unit);
				if (item==null) {
					throw new TimeoutException("Channel timed out waiting for items");
				}
		} catch (InterruptedException e) {
		}
		return item;
	}
	
	T get(long timeout) throws TimeoutException {
		return(get(timeout, TimeUnit.MILLISECONDS));
	}
	
	void add(T item)
	{
		try {
			q.put(item);
		} catch (InterruptedException e) {
		}
	}

	public void close()
	{
		logger.trace("Channel close()");
	}

	int getCount()
	{
		return q.size();
	}

}