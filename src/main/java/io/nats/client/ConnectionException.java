package io.nats.client;

public class ConnectionException extends Exception {

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

	public ConnectionException(String string, NATSException e) {
		super(string, e);
	}
}
