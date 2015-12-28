package io.nats.client;

import java.io.IOException;

public class NoServersException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NoServersException() {
		this("No servers available for connection");
	}

	public NoServersException(String message) {
		super(message);
	}

	public NoServersException(Throwable cause) {
		super(cause);
	}

	public NoServersException(String message, Throwable cause) {
		super(message, cause);
	}

}
