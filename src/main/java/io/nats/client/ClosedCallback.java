package io.nats.client;

public interface ClosedCallback {
	void onClose(ConnectionEvent event);
}
