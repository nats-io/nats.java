package io.nats.client;

public interface DisconnectedCallback {
	void onDisconnect(ConnectionEvent event);
}
