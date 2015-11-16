package io.nats.client;

public interface ConnEventHandler {
	public void onEvent(ConnEventArgs eventInfo);
}
