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
	Queue<T> q;
	T defaultVal = null;
	final Lock qLock = new ReentrantLock();
	final Condition hasItems = qLock.newCondition();

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

	T get(long timeout) throws TimeoutException {
		qLock.lock();
		try {
			if (finished) {
				return this.defaultVal;
			}

			if (q.size() > 0) {
				return q.poll();
			} else {
				if (timeout < 0) {
					while (q.size() == 0) {
						try {hasItems.await(); } catch (InterruptedException e) {}
					}
				} else {
					boolean stillWaiting = true;
					Date deadline = new Date(System.currentTimeMillis() + timeout);
					while (q.size()==0)
					{
						if (!stillWaiting)
							break;
						try {
							stillWaiting = hasItems.awaitUntil(deadline);
						} catch (InterruptedException e) {}
					}
					if(!stillWaiting) {
						throw new TimeoutException("Channel timed out waiting for items");
					}
				}

				if (finished) {
					return this.defaultVal;
				}					
				T item = q.poll();

				return item;
			}

		} finally {
			qLock.unlock();
		}
	} // get

	void add(T item)
	{
		qLock.lock();
		try
		{
			q.add(item);

			// if the queue count was previously zero, we were
			// waiting, so signal.
			if (q.size() <= 1)
			{
				hasItems.signal();
			}
		} finally {
			qLock.unlock();
		}
	}

	void close()
	{
		qLock.lock();
		try
		{
			finished = true;
			hasItems.signal();
		} finally {
			qLock.unlock();
		}
	}

	int getCount()
	{
		qLock.lock();
		try
		{
			return q.size();
		} finally {
			qLock.unlock();
		}
	}

}