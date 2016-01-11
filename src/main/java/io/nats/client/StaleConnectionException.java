/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.io.IOException;

public class StaleConnectionException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public StaleConnectionException() {
		this("nats: Stale Connection");
	}

	public StaleConnectionException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	public StaleConnectionException(Throwable e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	public StaleConnectionException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
