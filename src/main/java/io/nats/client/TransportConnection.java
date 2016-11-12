/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

interface TransportConnection {
    /**
     * Opens a connection using the specified URL string.
     *
     * @param url     the URL or URI of the transport endpoint
     * @param timeout the timeout value to be used in milliseconds
     */
    void open(String url, int timeout) throws IOException;

    /**
     * Closes the the connection and releases any held resources.
     */
    void close();

    /**
     * Returns a buffered reader for this connection.
     *
     * @return the {@code BufferedReader}
     */
    BufferedReader getBufferedReader();

    /**
     * Returns the buffered input stream for this connection.
     *
     * @param bufferSize the desired buffer size
     * @return the buffered input stream
     */
    InputStream getInputStream(int bufferSize);

    /**
     * Returns the buffered output stream for this connection.
     *
     * @param bufferSize the desired buffer size
     * @return the buffered output stream
     */
    OutputStream getOutputStream(int bufferSize);

    /**
     * Returns the closed state of the connection.
     *
     * @return {@code true} if the connection is closed, otherwise {@code false}
     */
    boolean isClosed();

    /**
     * Returns the connected state of the connection.
     *
     * @return {@code true} if the connection is connected, otherwise {@code false}
     */
    boolean isConnected();

}
