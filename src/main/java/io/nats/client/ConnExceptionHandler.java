package io.nats.client;

public interface ConnExceptionHandler {
	public void handleException(ConnectionImpl conn, java.lang.Throwable e);

	public void onError(ConnExceptionArgs eventArgs);

}
