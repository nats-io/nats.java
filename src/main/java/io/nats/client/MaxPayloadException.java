/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

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
