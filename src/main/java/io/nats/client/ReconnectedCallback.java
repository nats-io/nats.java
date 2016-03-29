/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

/**
 * When a previously disconnected {@code Connection} is reconnected to a NATS server, the
 * {@code Connection} object's {@code ReconnectedCallback} is notified, if one has been registered.
 */

public interface ReconnectedCallback {
    /**
     * This callback notification method is invoked when the {@code Connection} is successfully
     * reconnected.
     * 
     * @param event contains information pertinent to the reconnect event.
     * @see ConnectionFactory#setReconnectedCallback(ReconnectedCallback)
     * @see Connection#setReconnectedCallback(ReconnectedCallback)
     */
    void onReconnect(ConnectionEvent event);
}
