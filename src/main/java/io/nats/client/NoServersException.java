/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.io.IOException;

public class NoServersException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NoServersException() {
		this("nats: No servers available for connection");
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
