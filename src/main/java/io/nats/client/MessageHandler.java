package io.nats.client;

public interface MessageHandler {
	
	public void onMessage(Message msg);

}
