/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

/**
 * A {@code ConnectionEvent} object contains information about about an asynchronous event that has
 * occurred on a connection.
 */
public class ConnectionEvent {
    private final Connection nc;

    ConnectionEvent(Connection conn) {
        if (conn == null) {
            throw new IllegalArgumentException("nats: connection cannot be null");
        }
        this.nc = conn;
    }

    /**
     * Returns the {@link Connection} object on which the event occurred.
     *
     * @return the {@link Connection}
     */
    public Connection getConnection() {
        return nc;
    }

}
