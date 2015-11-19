package io.nats.client;

import java.util.concurrent.TimeUnit;

final class SyncSubscriptionImpl extends SubscriptionImpl implements SyncSubscription {

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
			if (conn == null) {
				throw new ConnectionClosedException();
			}
			if (mch == null) {
				throw new BadSubscriptionException();
			}
			if (sc == true) {
				sc = false;
				throw new SlowConsumerException();
			}
			localConn = (ConnectionImpl) this.getConnection();
			localChannel = mch;
			localMax = getMax();
		} finally {
			mu.unlock();
		}

		try {
			if (timeout >= 0) {
				msg = localChannel.get(timeout);
			} else {
				msg = localChannel.get(-1);
			}
		} catch (TimeoutException e) {
			if (localConn.exceptionHandler != null) {
				localConn.exceptionHandler.handleException(localConn, this, e);
			}
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
