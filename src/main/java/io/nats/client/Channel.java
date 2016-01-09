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

class Channel<T> implements AutoCloseable {

	final static Logger logger = LoggerFactory.getLogger(Channel.class);
	/**
	 * This channel class really a blocking queue, is named the way it is so the
	 * code more closely reads with GO.
	 */
	LinkedBlockingQueue<T> q;
	T defaultVal = null;
//	final Object mu = new Object(); 

	boolean finished = false;

	Channel() {
		q = new LinkedBlockingQueue<T>();
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
	
	T get(long timeout) throws TimeoutException {
		T item = defaultVal;
		if (finished) {
			return this.defaultVal;
		}
//		if (q.size() > 0) {
//			try { item = q.take(); } catch (InterruptedException e) {}
//			return item;
//		}
//		else
		try {
			if (timeout < 0)
				item = q.take();
			else
				item = q.poll(timeout, TimeUnit.MILLISECONDS);
				if (item==null) {
					throw new TimeoutException("Channel timed out waiting for items");
				}
		} catch (InterruptedException e) {
		}
		return item;
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
		finished = true;
		logger.trace("Channel.close: notifying");
	}

	int getCount()
	{
		return q.size();
	}

}