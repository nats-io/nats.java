package io.nats.client;

public interface DisconnectedEventHandler extends ConnectionEventHandler {
	public void onDisconnect(ConnectionEvent event);
}
