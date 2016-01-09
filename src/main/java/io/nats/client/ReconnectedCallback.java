package io.nats.client;

public interface ReconnectedCallback {
	void onReconnect(ConnectionEvent event);
}
