package io.nats.client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SubscriptionImpl implements Subscription, AutoCloseable {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	//	public SubscriptionImpl() {}

	final Lock mu = new ReentrantLock();

	long	sid; //int64 in Go
	//
	// Subject that represents this subscription. This can be different
	// than the received subject inside a Msg if this is a wildcard.
	String subject				= null;

	// Optional queue group name. If present, all subscriptions with the
	// same name will form a distributed queue, and each message will
	// only be processed by one member of the group.
	String queue;

	long msgs; //  uint64
	AtomicLong delivered = new AtomicLong(); // uint64
	long bytes; //     uint64
	long max; //       uint64
	int maxChannelLength;

	// slow consumer
	boolean sc;

	ConnectionImpl conn 			= null;
//	Channel<Message> mch = new Channel<Message>();
	Channel<Message> mch;

	public SubscriptionImpl(ConnectionImpl conn, String subject, String queue, int chanLen) {
		this.conn = conn;
		this.subject = subject;
		this.queue = queue;
		this.maxChannelLength = chanLen;
		this.mch = new Channel<Message>(this.maxChannelLength);
	}

	void closeChannel() {
		mu.lock();
		try {
			mch.close();
			mch = null;
		} finally {
			mu.unlock();
		}
	}

	@Override
	public String getSubject() {
		return subject;
	}

	public String getQueue() {
		if (queue==null)
			return "";
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
			logger.trace("getMax()={}, msgs={}",
					getMax(), msgs);
			if (getMax() > 0 && msgs > getMax())
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
//	boolean addMessage(Message m, int maxCount)
	boolean addMessage(Message m)
	{
		logger.trace("Entered addMessage({}, count={} max={}", 
				m, 
				mch.getCount(),
				maxChannelLength);
		if (mch != null)
		{
			if (mch.getCount() >= maxChannelLength)
			{
				logger.debug("MAXIMUM COUNT ({}) REACHED FOR SID: {}",
						maxChannelLength, getSid());
				return false;
			}
			else
			{
				sc = false;
				mch.add(m);
				logger.trace("Added message to channel: " + m);
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
	public void unsubscribe() throws IllegalStateException, IOException
	{
		ConnectionImpl c;
		mu.lock();
		try
		{
			c = this.conn;
		} finally {
			mu.unlock();
		}

		if ((c == null)||c.isClosed())
			throw new ConnectionClosedException("Not connected.");

		c.unsubscribe(this, 0);
	}
	
	
	@Override 
	public void close() {
		try {
			unsubscribe();
		} catch (IllegalStateException | IOException e) {
			// Just ignore. This is for AutoCloseable.
		}
	}

	/**
	 * @return the sid
	 */
	public long getSid() {

		return sid;
	}

	/**
	 * @param l the sid to set
	 */
	public void setSid(long id) {
		this.sid = id;
	}

	/**
	 * @return the max
	 */
	public long getMax() {
		return max;
	}

	/**
	 * @param max the max to set
	 */
	public void setMax(long max) {
		this.max = max;
	}

	@Override
	public Connection getConnection() {
		return (Connection)this.conn;
	}

	public void setConnection(ConnectionImpl conn) {
		this.conn = conn;
	}

	@Override
	public void autoUnsubscribe(int max) throws IOException {
		ConnectionImpl c = null;

		mu.lock();
		try
		{
			if ((conn == null)|| conn.isClosed())
				throw new ConnectionClosedException();
//				throw new BadSubscriptionException();

			c = conn;
		} finally {
			mu.unlock();
		}

		c.unsubscribe((Subscription)this, max);

	}

	public int getQueuedMessageCount() {
		if (this.mch != null)
			return this.mch.getCount();
		else
			return 0;
	}

	public String toString() {
		String s = String.format("{subject=%s, sid=%d, queued=%d, max=%d}",
				getSubject(), getSid(), getQueuedMessageCount(), getMax());
		return s;
	}

	protected abstract boolean processMsg(Message msg);
}
