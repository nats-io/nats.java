/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.nats.client.Constants.*;

abstract class SubscriptionImpl implements Subscription {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	final Lock mu = new ReentrantLock();

	long	sid; //int64 in Go
	
	// Subject that represents this subscription. This can be different
	// than the received subject inside a Msg if this is a wildcard.
	String subject				= null;

	// Optional queue group name. If present, all subscriptions with the
	// same name will form a distributed queue, and each message will
	// only be processed by one member of the group.
	String queue;

	// Number of messages delivered on this subscription
	long msgs; 
	AtomicLong delivered = new AtomicLong(); // uint64
	long bytes; // uint64
	int pendingMax; // uint64 in Go, int here due to underlying data structure
	int max; //
	
	protected boolean closed;
	protected boolean connClosed;

	// slow consumer flag
	boolean sc;

	ConnectionImpl conn 			= null;
	Channel<Message> mch;

	SubscriptionImpl(ConnectionImpl conn, String subject, String queue, int maxPending) {
		this.conn = conn;
		this.subject = subject;
		this.queue = queue;
//		if (conn != null) {
//			this.pendingMax = conn.getOptions().getMaxPendingMsgs();
//		}
		this.setMaxPending(maxPending);
		this.mch = new Channel<Message>(maxPending);
	}

	void closeChannel() {
		mu.lock();
		try {
			if (mch != null) {
				mch.close();
				mch = null;
			}
		} finally {
			mu.unlock();
		}
	}

	@Override
	public String getSubject() {
		return subject;
	}

	public String getQueue() {
//		if (queue==null)
//			return "";
		return queue;
	}

	public Channel<Message> getChannel() {
		return this.mch;
	}

	public void setChannel(Channel<Message> ch) {
		this.mch = ch;
	}

	public boolean tallyMessage(long length) {
		mu.lock();
		try
		{
//			logger.trace("getMax()={}, msgs={}",
//					max, msgs);
			if (max > 0 && msgs > max)
				return true;

			this.msgs++;
			this.bytes += bytes;

		} finally {
			mu.unlock();
		}

		return false;

	}

	// returns false if the message could not be added because
	// the channel is full, true if the message was added
	// to the channel.
	boolean addMessage(Message m)
	{
//		logger.trace("Entered addMessage({}, count={} max={}", 
//				m, 
//				mch.getCount(),
//				max);
		if (mch != null)
		{
			if (mch.getCount() >= getMaxPending())
			{
//				logger.trace("MAXIMUM COUNT ({}) REACHED FOR SID: {}",
//						max, getSid());
				return false;
			}
			else
			{
				sc = false;
				mch.add(m);
//				logger.trace("Added message to channel: " + m);
			}
		} // mch != null
		return true;
	}

	public boolean isValid() {
		mu.lock();
		try {
			return (conn != null);
		} finally {
			mu.unlock();
		}
	}

	@Override
	public void unsubscribe() throws IOException
	{
		ConnectionImpl c;
		mu.lock();
		try
		{
			c = this.conn;
		} finally {
			mu.unlock();
		}
		if (c == null)
			throw new IllegalStateException(ERR_BAD_SUBSCRIPTION);
		c.unsubscribe(this, 0);
	}
	
	@Override
	public void autoUnsubscribe(int max) throws IOException {
		ConnectionImpl c = null;

		mu.lock();
		try {
			if (conn == null)
				throw new IllegalStateException(ERR_BAD_SUBSCRIPTION);
			c = conn;
		} finally {
			mu.unlock();
		}

		c.unsubscribe(this, max);
	}

	@Override 
	public void close() {
		try {
			logger.trace("Calling unsubscribe from AutoCloseable.close()");
			unsubscribe();
		} catch (Exception e) {
			// Just ignore. This is for AutoCloseable.
		}
	}

	protected long getSid() {

		return sid;
	}

	protected void setSid(long id) {
		this.sid = id;
	}

	/**
	 * @return the maxPending
	 */
	protected int getMaxPending() {
		return this.pendingMax;
	}

	/**
	 * @param pending the pending to set
	 */
	protected void setMaxPending(int pending) {
		this.pendingMax = pending;
		if (pending<=0) {
			pendingMax = ConnectionFactory.DEFAULT_MAX_PENDING_MSGS;
		}
	}

	protected Connection getConnection() {
		return (Connection)this.conn;
	}

	protected void setConnection(ConnectionImpl conn) {
		this.conn = conn;
	}

	public int getQueuedMessageCount() {
		if (this.mch != null)
			return this.mch.getCount();
		else
			return 0;
	}

	public String toString() {
		String s = String.format("{subject=%s, queue=%s, sid=%d, queued=%d, max=%d, valid=%b}",
				getSubject(), 
				getQueue()==null ? "null" : getQueue(), 
				getSid(), getQueuedMessageCount(), getMaxPending(),
				isValid());
		return s;
	}

	protected void setSlowConsumer(boolean sc) {
		this.sc = sc;
	}
	
	protected boolean isSlowConsumer() {
		return this.sc;
	}

	protected boolean processMsg(Message msg) {
		return true;
	}

	protected void setMax(int max) {
		this.max = max;
	}
	
	Lock getLock() {
		return this.mu;
	}
}
