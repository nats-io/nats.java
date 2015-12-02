package io.nats.client;

import java.io.IOException;

public class StaleConnectionException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public StaleConnectionException() {
		// TODO Auto-generated constructor stub
	}

	public StaleConnectionException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	public StaleConnectionException(Throwable e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	public StaleConnectionException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
