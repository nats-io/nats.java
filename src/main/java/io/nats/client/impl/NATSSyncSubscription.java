package io.nats.client.impl;

import io.nats.client.Message;
import io.nats.client.SyncSubscription;

public class NATSSyncSubscription extends NATSSubscription implements SyncSubscription {

	public NATSSyncSubscription(NATSConnection conn, String subject, String queue) {
		super(conn, subject, queue);
		// TODO Auto-generated constructor stub
	}

	public NATSMessage nextMsg() {
		//TODO implement
		return null;
	}
	
	public NATSMessage nextMsg(int timeoutMsec) {
		//TODO implement
		return null;
	}

	@Override
	public Message nextMsg(long timeout) {
		// TODO Auto-generated method stub
		return (Message)null;
	}
	
}
