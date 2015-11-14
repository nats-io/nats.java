package io.nats.client;

import java.util.concurrent.TimeUnit;

public class SyncSubscriptionImpl extends AbstractSubscriptionImpl implements SyncSubscription {

	public SyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue) {
		super(nc, subj, queue);
	}

	@Override
	public Message nextMessage()
			throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException {
		return nextMessage(-1);
	}

	@Override
	public Message nextMessage(long timeout)
			throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException {
		Message msg = null;
		ConnectionImpl localConn;
		Channel<Message> localChannel;
		long localMax;

		mu.lock();
		try {
			if (mch == null) {
				throw new ConnectionClosedException();
			}
			if (conn == null) {
				throw new BadSubscriptionException();
			}
			if (sc == true) {
				sc = false;
				throw new SlowConsumerException();
			}
			localConn = (ConnectionImpl) this.getConnection();
			localChannel = this.mch;
			localMax = this.getMax();

		} finally {
			mu.unlock();
		}

		try {
			if (timeout >= 0) {
				msg = localChannel.poll(timeout, TimeUnit.MILLISECONDS);
			} else {
				msg = localChannel.take();
			}
		} catch (InterruptedException e) {
		}

		if (msg != null) {
			long d = this.delivered.incrementAndGet();
			if (d == max) {
				// Remove subscription if we have reached max.
				localConn.removeSub(this);
			}
			if (localMax > 0 && d > localMax) {
				throw new MaxMessagesException("nats: Max messages delivered");
			}
		}

		return msg;

	}

	@Override
	protected boolean processMsg(Message msg) {
		return true;
	}
}
