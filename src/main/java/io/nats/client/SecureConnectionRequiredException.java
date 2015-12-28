package io.nats.client;

import java.io.IOException;

public class SecureConnectionRequiredException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public SecureConnectionRequiredException() {
		this("A secure connection is required by the server.");
	}

	public SecureConnectionRequiredException(String msg) {
		super(msg);
	}

	public SecureConnectionRequiredException(Throwable e) {
		super(e);
	}

	public SecureConnectionRequiredException(String string, Exception e) {
		super(string, e);
	}

}
