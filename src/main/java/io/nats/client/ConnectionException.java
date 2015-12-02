package io.nats.client;

import java.io.IOException;

public class ConnectionException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7773607460353137077L;

	public ConnectionException() {
		super();
	}

	public ConnectionException(String message) {
		super(message);
	}

	public ConnectionException(String string, Throwable e) {
		super(string, e);
	}

	public ConnectionException(Exception e) {
		super(e);
	}
}
