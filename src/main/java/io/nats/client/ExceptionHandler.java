package io.nats.client;

public interface ExceptionHandler {
	public void handleException(ConnectionImpl conn, java.lang.Throwable e);

}
