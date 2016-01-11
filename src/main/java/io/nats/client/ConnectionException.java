/*******************************************************************************
 * Copyright (c) 2012, 2015 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.io.IOException;

public class ConnectionException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ConnectionException() {
		super();
	}

	public ConnectionException(String message) {
		super(message);
	}

	public ConnectionException(String string, Throwable e) {
		super(string, e);
	}

	public ConnectionException(Exception e) {
		super(e);
	}
}
