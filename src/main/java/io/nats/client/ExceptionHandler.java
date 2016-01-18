/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

/**
 * If NATS detects a serious problem with a {@code Connection} object, it informs 
 * the {@code Connection} object's {@code ExceptionHandler}, if one has been registered.
 * It does this by calling the listener's {@code onException} method, passing it a 
 * NATSException argument describing the problem.
 * <p>
 * An exception handler allows a client to be notified of a problem asynchronously. 
 * Some connections only consume messages asynchronously, so they would have no other 
 * way to learn that their connection has failed.
 * @see Connection#setExceptionHandler(ExceptionHandler)
 * @see ConnectionFactory#setExceptionHandler(ExceptionHandler)
 */
public interface ExceptionHandler {
	/**
	 * Notify user of a NATS exception
	 * @param e a {@code NATSException}, wrapping the original event along with other 
	 * metadata
	 */
	public void onException(NATSException e);

}
