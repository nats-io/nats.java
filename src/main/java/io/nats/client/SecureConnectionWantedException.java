package io.nats.client;

public class SecureConnectionWantedException extends Exception {

	public SecureConnectionWantedException() {
		this("A secure connection is required by the client.");
	}

	public SecureConnectionWantedException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public SecureConnectionWantedException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public SecureConnectionWantedException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public SecureConnectionWantedException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
