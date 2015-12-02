package io.nats.client;

public interface ClosedEventHandler extends ConnectionEventHandler {
	public void onClose(ConnectionEvent event);
}
