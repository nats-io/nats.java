package io.nats.client;

public class BadSubscriptionException extends IllegalStateException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public BadSubscriptionException() {
		// TODO Auto-generated constructor stub
	}

	public BadSubscriptionException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	public BadSubscriptionException(Throwable e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	public BadSubscriptionException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
