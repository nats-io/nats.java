package io.nats.client;

public interface ErrorEventHandler {
	public void onError(ConnExceptionArgs error);
}
