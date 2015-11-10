package io.nats.client;

import io.nats.client.impl.NATSConnection;

public interface ExceptionHandler {
	public void handleException(NATSConnection conn, java.lang.Throwable e);

}
