/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

public class ConnectionClosedException extends IllegalStateException {
	final static String DEFAULT_MSG = "nats: Connection Closed";


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ConnectionClosedException() {
		// TODO Auto-generated constructor stub
		super(DEFAULT_MSG);
	}
	
	public ConnectionClosedException(String msg) {
		super(msg);
	}
	
	public ConnectionClosedException(Throwable e) {
		super(e);
	}
	
	public ConnectionClosedException(String msg, Throwable e) {
		super(msg, e);
	}
	
}
