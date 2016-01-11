/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.io.IOException;

public class SecureConnectionRequiredException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public SecureConnectionRequiredException() {
		this("nats: Secure Connection required");
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
