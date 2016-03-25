/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

/**
 * A {@code ConnectionEvent} object contains information about about an asynchronous event that has
 * occurred on a connection.
 *
 */
public class ConnectionEvent {
    Connection nc;

    protected ConnectionEvent(Connection conn) {
        this.nc = conn;
    }

    /**
     * @return the {@code Connection} object for which the event has occurred.
     */
    public Connection getConnection() {
        return nc;
    }

}
