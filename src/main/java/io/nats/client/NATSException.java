package io.nats.client;

public class NATSException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4261161539086944590L;

	NATSException() {
		super();
	}
	
	public NATSException(String msg) {
		super(msg);
	}
	
	NATSException(Throwable e) {
		super(e);
	}

	public NATSException(String string, Exception e) {
		super(string, e);
	}
}
