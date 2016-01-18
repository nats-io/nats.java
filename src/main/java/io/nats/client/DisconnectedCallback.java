/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

/**
 * When a {@code Connection} is disconnected for any reason, the {@code Connection} 
 * object's {@code DisconnectedCallback} is notifed, if one has been registered. 
 */
public interface DisconnectedCallback {
	/**
	 * This callback notification method is invoked when the {@code Connection} is 
	 * disconnected.
	 * @param event contains information pertinent to the disconnect event.
	 * @see ConnectionFactory#setDisconnectedCallback(DisconnectedCallback)
	 * @see Connection#setDisconnectedCallback(DisconnectedCallback)
	 */
	void onDisconnect(ConnectionEvent event);
}
