package io.nats.client;

import java.io.IOException;

public class SecureConnRequiredException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public SecureConnRequiredException() {
		// TODO Auto-generated constructor stub
	}

	public SecureConnRequiredException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	public SecureConnRequiredException(Throwable e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	public SecureConnRequiredException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
