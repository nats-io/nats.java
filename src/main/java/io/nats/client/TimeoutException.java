package io.nats.client;

public class TimeoutException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1254515007904069462L;

	public TimeoutException() {
		// TODO Auto-generated constructor stub
	}

	public TimeoutException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	public TimeoutException(Throwable e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	public TimeoutException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
