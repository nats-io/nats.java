package io.nats.client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SubscriptionImpl implements Subscription {

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
	//		mch       chan *Msg

	// slow consumer
	boolean sc;

	ConnectionImpl conn 			= null;
	Channel<Message> mch = new Channel<Message>();

	SubscriptionImpl() {

	}

	public SubscriptionImpl(ConnectionImpl conn, String subject, String queue) {
		this.conn = conn;
		this.subject = subject;
		this.queue = queue;
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

	public String getSubject() {
		return subject;
	}

	public String getQueue() {
		if (queue==null)
			return "";
		return queue;
	}

	public boolean tallyMessage(long length) {
		mu.lock();
		try
		{
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
	boolean addMessage(Message m, int maxCount)
	{
		if (logger.isDebugEnabled())
			logger.debug("Entered addMessage(" + m + ", " + maxCount + ")");
		if (mch != null)
		{
			if (mch.getCount() >= maxCount)
			{
				if (logger.isDebugEnabled())
					logger.debug("MAXIMUM COUNT " + maxCount 
							+ " REACHED for sid:" + getSid());
				return false;
			}
			else
			{
				sc = false;
				mch.add(m);
				if (logger.isDebugEnabled())
					logger.debug("Added message to channel: " + m);
			}
		} // mch != null
		return true;
	}

	public boolean isValid() {
		boolean rv = false;
		mu.lock();
		try {
			rv = (conn != null);
		} finally {
			mu.unlock();
		}
		return rv;
	}

	@Override
	public void unsubscribe() throws ConnectionClosedException, BadSubscriptionException, IOException {
		ConnectionImpl c;
		mu.lock();
		try
		{
			c = this.conn;
		} finally {
			mu.unlock();
		}

		if (c == null)
			throw new BadSubscriptionException();

		c.unsubscribe(this, 0);		
	}

	public int queuedMsgs() {
		return 0;
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
	public void setSid(long l) {
		this.sid = l;
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

	@Override
	public void autoUnsubscribe(int max) throws BadSubscriptionException, ConnectionClosedException, IOException {
        ConnectionImpl c = null;

        mu.lock();
        try
        {
            if (conn == null)
                throw new BadSubscriptionException();

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
