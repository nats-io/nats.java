/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
/**
 * 
 */
package io.nats.client;

import java.io.IOException;

/**
 * 
 *
 */
public class AuthorizationException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public AuthorizationException() {
		this("nats: Authorization Failed");
	}

	public AuthorizationException(String arg0) {
		super(arg0);
	}

	public AuthorizationException(Throwable arg0) {
		super(arg0);
	}

	public AuthorizationException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
