package io.nats.client;

public interface ConnectionEventHandler {
	public void onEvent(ConnectionEvent eventInfo);
}
