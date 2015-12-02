package io.nats.client;

import java.io.IOException;

public class ConnectionClosedException extends IllegalStateException {
	final static String DEFAULT_MSG = "Connection closed.";


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ConnectionClosedException() {
		// TODO Auto-generated constructor stub
		super(DEFAULT_MSG);
	}
	
	public ConnectionClosedException(String msg) {
		super(msg);
	}
	
	public ConnectionClosedException(Throwable e) {
		super(e);
	}
	
	public ConnectionClosedException(String msg, Throwable e) {
		super(msg, e);
	}
	
}
