package io.nats.client;

public class StaleConnectionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 898561752318745558L;

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
