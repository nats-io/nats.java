package io.nats.client;

public interface ReconnectedEventHandler extends ConnectionEventHandler {
	public void onReconnect(ConnectionEvent event);

}
