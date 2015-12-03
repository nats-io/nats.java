package io.nats.client;

public interface ExceptionHandler {
	public void onException(Connection conn, Subscription sub, Throwable e);

}
