/**
 * 
 */
package io.nats.client;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Larry McQueary
 *
 */
public class SubscriptionImpl implements Subscription {
	
	protected final Lock mu = new ReentrantLock();

	private long	sid; //int64 in Go
//
	// Subject that represents this subscription. This can be different
	// than the received subject inside a Msg if this is a wildcard.
	private String subject				= null;

	// Optional queue group name. If present, all subscriptions with the
	// same name will form a distributed queue, and each message will
	// only be processed by one member of the group.
	String queue;

	private long msgs; //  uint64
	private long delivered; // uint64
	private long bytes; //     uint64
	private long max; //       uint64
	MessageHandler mcb;
//	mch       chan *Msg
	
	// slow consumer
	boolean sc;
	
	protected ConnectionImpl conn 			= null;
	protected Queue<MessageImpl> mch 			= new LinkedBlockingQueue<MessageImpl>();
	
	public SubscriptionImpl(ConnectionImpl conn, String subject, String queue) {
		this.conn = conn;
		this.subject = subject;
		this.queue = queue;
	}
	
	public SubscriptionImpl(String subj, String queue, MessageHandler cb, ConnectionImpl nc) {
		// TODO Auto-generated constructor stub
		this.conn=nc;
		this.subject = subj;
		this.queue = queue;
		this.mcb = cb;
	}

	void closeChannel() {
		mu.lock();
		try {
			//TODO Is this necessary?
			mch.clear();
		} finally {
			mu.unlock();
		}
	}
	
	public String getSubject() {
		return subject;
	}
	
	public String getQueue() {
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
    boolean addMessage(MessageImpl msg, int maxCount)
    {
        if (mch != null)
        {
            if (mch.size() >= maxCount)
            {
                return false;
            }
            else
            {
                sc = false;
                mch.add(msg);
            }
        }
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
	 * @param sid the sid to set
	 */
	public void setSid(long sid) {
		this.sid = sid;
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

	public boolean processMsg(MessageImpl m) {
		if (mcb==null) {
			return false;
		}
		mcb.onMessage(m);
		return true;
	}

	@Override
	public Connection getConnection() {
		return (Connection)this.conn;
	}

	@Override
	public void autoUnsubscribe(int max) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getQueuedMessageCount() {
		// TODO Auto-generated method stub
		return 0;
	}


}
