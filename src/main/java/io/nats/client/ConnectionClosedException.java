package io.nats.client;

public class ConnectionClosedException extends Exception {
	final static String DEFAULT_MSG = "Connection closed.";
	public ConnectionClosedException() {
		// TODO Auto-generated constructor stub
		super(DEFAULT_MSG);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 8588464408154143356L;


}
