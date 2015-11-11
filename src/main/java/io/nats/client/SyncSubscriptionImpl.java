package io.nats.client;

public class SyncSubscriptionImpl extends SubscriptionImpl implements SyncSubscription {

	public SyncSubscriptionImpl(ConnectionImpl conn, String subject, String queue) {
		super(conn, subject, queue);
		// TODO Auto-generated constructor stub
	}

	public MessageImpl nextMsg() {
		//TODO implement
		return null;
	}
	
	public MessageImpl nextMsg(int timeoutMsec) 
			throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException {
		MessageImpl msg = null;
		mu.lock();
		try {
			if (mch == null) {
				throw new ConnectionClosedException();
			}
			if (mcb != null) {
				throw new UnsupportedOperationException("Illegal call on an async Subscription");
			}
			if (conn == null) {
				throw new BadSubscriptionException();
			}
			if (sc == true) {
				sc = false;
				throw new SlowConsumerException();
			}			
		} finally {
			mu.unlock();
		}
		
		return msg;
	}

	@Override
	public Message nextMsg(long timeout) {
		// TODO Auto-generated method stub
		return (Message)null;
	}
	
}
