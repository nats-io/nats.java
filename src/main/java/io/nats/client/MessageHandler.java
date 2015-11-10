package io.nats.client;

import io.nats.client.impl.NATSMessage;

public interface MessageHandler {
	
	public void onMessage(NATSMessage msg);

}
