/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

/**
 * A MessageHandler object is used as a callback to receive asynchronously 
 * delivered messages.
 *
 */
public interface MessageHandler {
	
	/**
	 * Passes a message to the handler.
	 * @param msg - the received Message that triggered the callback
	 * invocation.
	 */
	void onMessage(Message msg);

}
