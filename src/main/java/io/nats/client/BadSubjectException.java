/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

/**
 * Thrown when a malformed subject name has been passed as a 
 * parameter
 * @see IllegalArgumentException
 */
public class BadSubjectException extends IllegalArgumentException {

	private static final long serialVersionUID = 1L;

	/**
	 * @see IllegalArgumentException#IllegalArgumentException()
	 */
	public BadSubjectException() {
		this("nats: Invalid Subject");
	}

	/**
	 * @param s the detail message
	 * @see IllegalArgumentException#IllegalArgumentException(String)
	 */
	public BadSubjectException(String s) {
		super(s);
	}

	/**
	 * @param cause the cause
	 * @see IllegalArgumentException#IllegalArgumentException(Throwable)
	 * @param cause the cause
	 */
	public BadSubjectException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message the detail message
	 * @param cause the cause
	 * @see IllegalArgumentException#IllegalArgumentException(String, Throwable)
	 */
	public BadSubjectException(String message, Throwable cause) {
		super(message, cause);
	}

}
