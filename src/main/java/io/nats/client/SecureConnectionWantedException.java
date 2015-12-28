package io.nats.client;

import java.io.IOException;

public class SecureConnectionWantedException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public SecureConnectionWantedException() {
		this("A secure connection is required by the client.");
	}

	public SecureConnectionWantedException(String message) {
		super(message);
	}

	public SecureConnectionWantedException(Throwable cause) {
		super(cause);
	}

	public SecureConnectionWantedException(String message, Throwable cause) {
		super(message, cause);
	}

}
