/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

/**
 * When a {@code Connection} is closed for any reason, the {@code Connection} object's
 * {@code ClosedCallback} is notifed, if one has been registered.
 */
public interface ClosedCallback {
    /**
     * This callback notification method is invoked when the {@code Connection} is closed.
     * 
     * @param event contains information pertinent to the close event.
     * @see ConnectionFactory#setClosedCallback(ClosedCallback)
     * @see Connection#setClosedCallback(ClosedCallback)
     */
    void onClose(ConnectionEvent event);
}
