/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

/**
 * Thrown when a closed {@link Subscription} has been passed as a 
 * parameter to a method  
 */
public class BadSubscriptionException extends IllegalStateException {


	private static final long serialVersionUID = 1L;

	/**
	 * @see IllegalStateException#IllegalStateException()
	 */
	public BadSubscriptionException() {
		this("nats: Invalid Subscription");
	}

	/**
	 * @param msg the detail message.
	 * @see IllegalStateException#IllegalStateException(String)
	 */
	public BadSubscriptionException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param e the cause
	 * @see IllegalStateException#IllegalStateException(Throwable)
	 */
	public BadSubscriptionException(Throwable e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param string the detail message
	 * @param e the cause
	 * @see IllegalStateException#IllegalStateException(String, Throwable)
	 */
	public BadSubscriptionException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
