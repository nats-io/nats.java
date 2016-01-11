/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

class SyncSubscriptionImpl extends SubscriptionImpl implements SyncSubscription {

	public SyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue, int max) {
		super(nc, subj, queue, max);
	}

	@Override
	public Message nextMessage()
			throws IOException {
		Message m = null;
		try {
			m = nextMessage(-1);
		} catch (TimeoutException e) {
			// Can't happen
		}
		return m;
	}

	@Override
	public Message nextMessage(long timeout)
			throws IOException, TimeoutException {
		Message msg = null;
		ConnectionImpl localConn;
		Channel<Message> localChannel;
		long localMax;

		mu.lock();
		if (connClosed) {
			mu.unlock();
			throw new ConnectionClosedException();
		}
		if (mch == null) {
			if ((max > 0) && (delivered.get() >= max)) {
				mu.unlock();
				throw new MaxMessagesException();
			} else if (closed) {
				mu.unlock();
				throw new BadSubscriptionException();
			}
		}
		if (sc == true) {
			sc = false;
			mu.unlock();
			throw new SlowConsumerException();
		}
		localConn = (ConnectionImpl) this.getConnection();
		localChannel = mch;
		localMax = getMax();
		mu.unlock();

		if (timeout >= 0) {
			try {
				msg = localChannel.get(timeout);
			} catch (TimeoutException e) {
				throw e;
			}
		} else {
			msg = localChannel.get();
		}

		long d = delivered.incrementAndGet();
		if (localMax > 0) {
			if (d > localMax) {
				throw new MaxMessagesException();					
			}
			// Remove subscription if we have reached max.
			if (d == localMax) {
				localConn.mu.lock();
				localConn.removeSub(this);
				localConn.mu.unlock();
			}
		}
		return msg;

	}
}
