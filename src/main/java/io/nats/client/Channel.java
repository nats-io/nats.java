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

	synchronized T get(long timeout) throws TimeoutException {
		T item = null;
		if (finished) {
			return this.defaultVal;
		}

		if (q.size() > 0) {
			try { item = q.take(); } catch (InterruptedException e) {}
			return item;
		} else {
			if (timeout < 0) {
				try { this.wait(); } catch (InterruptedException e) {}
			} else {
				long t0 = System.nanoTime();
				try {this.wait(timeout);} catch (InterruptedException e) {}
				long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-t0);
				if (elapsed > timeout)
					throw new TimeoutException("Channel timed out waiting for items");					
			}

			if (finished) {
				return this.defaultVal;
			}	
			
			item = q.poll();
//			if (item==null)
//				throw new TimeoutException("Channel timed out waiting for items");

			return item;
		}

	} // get

	synchronized void add(T item)
	{
		q.add(item);

		// if the queue count was previously zero, we were
		// waiting, so signal.
		if (q.size() <= 1)
		{
			this.notify();
		}
	}

	synchronized void close()
	{
		finished = true;
		logger.trace("Channel.close: notifying");
		this.notify();
	}

	synchronized int getCount()
	{
		return q.size();
	}

}