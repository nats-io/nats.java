package io.nats.client;

public interface MessageHandler {
	
	/**
	 * @param msg - the received Message that triggered the callback
	 * invocation.
	 */
	void onMessage(Message msg);

}
