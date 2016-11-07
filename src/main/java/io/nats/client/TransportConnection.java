package io.nats.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeoutException;

/**
 * Created by larry on 11/3/16.
 */
interface TransportConnection {
    /**
     * Opens a connection using the specified URL string.
     *
     * @param url the URL or URI of the transport endpoint
     * @param timeout the timeout value to be used in milliseconds
     */
    void open(String url, int timeout) throws IOException, TimeoutException;

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
