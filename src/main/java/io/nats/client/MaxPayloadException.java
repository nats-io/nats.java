package io.nats.client;

import java.io.IOException;

public class MaxPayloadException extends IllegalArgumentException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MaxPayloadException() {
		this("nats: Maximum Payload Exceeded");
	}

	public MaxPayloadException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public MaxPayloadException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public MaxPayloadException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}
