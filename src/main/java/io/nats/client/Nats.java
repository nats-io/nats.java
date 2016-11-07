/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Nats {

    /**
     * Default server host.
     *
     * <p>This property is defined as String {@value #DEFAULT_HOST}
     */
    public static final String DEFAULT_HOST = "localhost";
    static final String PROP_PROPERTIES_FILENAME = "jnats.properties";
    static final String PROP_CLIENT_VERSION = "client.version";

    // Property names
    static final String PFX = "io.nats.client.";
    /**
     * This property is defined as String {@value #PROP_RECONNECTED_CB}.
     */
    public static final String PROP_RECONNECTED_CB = PFX + "callback.reconnected";
    /**
     * This property is defined as String {@value #PROP_DISCONNECTED_CB}.
     */
    public static final String PROP_DISCONNECTED_CB = PFX + "callback.disconnected";
    /**
     * This property is defined as String {@value #PROP_CLOSED_CB}.
     */
    public static final String PROP_CLOSED_CB = PFX + "callback.closed";
    /**
     * This property is defined as String {@value #PROP_EXCEPTION_HANDLER}.
     */
    public static final String PROP_EXCEPTION_HANDLER = PFX + "callback.exception";
    /**
     * This property is defined as String {@value #PROP_MAX_PINGS}.
     */
    public static final String PROP_MAX_PINGS = PFX + "maxpings";
    /**
     * This property is defined as String {@value #PROP_PING_INTERVAL}.
     */
    public static final String PROP_PING_INTERVAL = PFX + "pinginterval";
    /**
     * This property is defined as String {@value #PROP_CONNECTION_TIMEOUT}.
     */
    public static final String PROP_CONNECTION_TIMEOUT = PFX + "timeout";
    /**
     * This property is defined as String {@value #PROP_RECONNECT_BUF_SIZE}.
     */
    public static final String PROP_RECONNECT_BUF_SIZE = PFX + "reconnect.buffer.size";
    /**
     * This property is defined as String {@value #PROP_RECONNECT_WAIT}.
     */
    public static final String PROP_RECONNECT_WAIT = PFX + "reconnect.wait";
    /**
     * This property is defined as String {@value #PROP_MAX_RECONNECT}.
     */
    public static final String PROP_MAX_RECONNECT = PFX + "reconnect.max";
    /**
     * This property is defined as String {@value #PROP_RECONNECT_ALLOWED}.
     */
    public static final String PROP_RECONNECT_ALLOWED = PFX + "reconnect.allowed";
    /**
     * This property is defined as String {@value #PROP_TLS_DEBUG}.
     */
    public static final String PROP_TLS_DEBUG = PFX + "tls.debug";
    /**
     * This property is defined as String {@value #PROP_SECURE}.
     */
    public static final String PROP_SECURE = PFX + "secure";
    /**
     * This property is defined as String {@value #PROP_PEDANTIC}.
     */
    public static final String PROP_PEDANTIC = PFX + "pedantic";
    /**
     * This property is defined as String {@value #PROP_VERBOSE}.
     */
    public static final String PROP_VERBOSE = PFX + "verbose";
    /**
     * This property is defined as String {@value #PROP_CONNECTION_NAME}.
     */
    public static final String PROP_CONNECTION_NAME = PFX + "name";
    /**
     * This property is defined as String {@value #PROP_NORANDOMIZE}.
     */
    public static final String PROP_NORANDOMIZE = PFX + "norandomize";
    /**
     * This property is defined as String {@value #PROP_SERVERS}.
     */
    public static final String PROP_SERVERS = PFX + "servers";
    /**
     * This property is defined as String {@value #PROP_PASSWORD}.
     */
    public static final String PROP_PASSWORD = PFX + "password";
    /**
     * This property is defined as String {@value #PROP_USERNAME}.
     */
    public static final String PROP_USERNAME = PFX + "username";
    /**
     * This property is defined as String {@value #PROP_PORT}.
     */
    public static final String PROP_PORT = PFX + "port";
    /**
     * This property is defined as String {@value #PROP_HOST}.
     */
    public static final String PROP_HOST = PFX + "host";
    /**
     * This property is defined as String {@value #PROP_URL}.
     */
    public static final String PROP_URL = PFX + "url";

    private static final int SECOND = 1000;
    private static final int MINUTE = 60 * SECOND;

    /*
     * Constants
     */

    /**
     * Default server URL.
     *
     * <p>This property is defined as String {@value #DEFAULT_URL}
     */
    public static final String DEFAULT_URL = "nats://localhost:4222";

    /**
     * Default server port.
     *
     * <p>This property is defined as int {@value #DEFAULT_PORT}
     */
    public static final int DEFAULT_PORT = 4222;

    /**
     * Default maximum number of reconnect attempts.
     *
     * <p>This property is defined as String {@value #DEFAULT_MAX_RECONNECT}
     */
    public static final int DEFAULT_MAX_RECONNECT = 60;

    /**
     * Default wait time before attempting reconnection to the same server.
     *
     * <p>This property is defined as String {@value #DEFAULT_RECONNECT_WAIT}
     */
    public static final int DEFAULT_RECONNECT_WAIT = 2 * SECOND;

    /**
     * Default connection timeout.
     *
     * <p>This property is defined as String {@value #DEFAULT_TIMEOUT}
     */
    public static final int DEFAULT_TIMEOUT = 2 * SECOND;

    /**
     * Default server ping interval. {@code <=0} means disabled.
     *
     * <p>This property is defined as String {@value #DEFAULT_PING_INTERVAL}
     */

    public static final int DEFAULT_PING_INTERVAL = 2 * MINUTE;
    /**
     * Default maximum number of pings that have not received a response.
     *
     * <p>This property is defined as String {@value #DEFAULT_MAX_PINGS_OUT}
     */
    public static final int DEFAULT_MAX_PINGS_OUT = 2;

    /**
     * Default of pending message buffer that is used for buffering messages that are published
     * during a disconnect/reconnect.
     *
     * <p>This property is defined as String {@value #DEFAULT_RECONNECT_BUF_SIZE}
     */
    public static final int DEFAULT_RECONNECT_BUF_SIZE = 8 * 1024 * 1024;

    static final String DEFAULT_LANG_STRING = "java";

    /**
     * Default SSL/TLS protocol version.
     *
     * <p>This property is defined as String {@value #DEFAULT_SSL_PROTOCOL}
     */
    static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";

    // Error messages
    // STALE_CONNECTION is for detection and proper handling of a stale connections
    static final String STALE_CONNECTION = "stale connection";
    // PERMISSIONS_ERR is for when nats server subject authorization has failed.
    static final String PERMISSIONS_ERR = "permissions violation";

    // Common messages
    /**
     * This error message is defined as String {@value #ERR_CONNECTION_CLOSED}.
     */
    public static final String ERR_CONNECTION_CLOSED = "nats: connection closed";
    /**
     * This error message is defined as String {@value #ERR_SECURE_CONN_REQUIRED}.
     */
    public static final String ERR_SECURE_CONN_REQUIRED = "nats: secure connection required";
    /**
     * This error message is defined as String {@value #ERR_SECURE_CONN_WANTED}.
     */
    public static final String ERR_SECURE_CONN_WANTED = "nats: secure connection not available";
    /**
     * This error message is defined as String {@value #ERR_BAD_SUBSCRIPTION}.
     */
    public static final String ERR_BAD_SUBSCRIPTION = "nats: invalid subscription";
    /**
     * This error message is defined as String {@value #ERR_BAD_SUBJECT}.
     */
    public static final String ERR_BAD_SUBJECT = "nats: invalid subject";
    /**
     * This error message is defined as String {@value #ERR_SLOW_CONSUMER}.
     */
    public static final String ERR_SLOW_CONSUMER = "nats: slow consumer, messages dropped";
    /**
     * This error message is defined as String {@value #ERR_TCP_FLUSH_FAILED}.
     */
    public static final String ERR_TCP_FLUSH_FAILED =
            "nats: i/o exception when flushing output stream";
    /**
     * This error message is defined as String {@value #ERR_TIMEOUT}.
     */
    public static final String ERR_TIMEOUT = "nats: timeout";
    /**
     * This error message is defined as String {@value #ERR_BAD_TIMEOUT}.
     */
    public static final String ERR_BAD_TIMEOUT = "nats: timeout invalid";
    /**
     * This error message is defined as String {@value #ERR_AUTHORIZATION}.
     */
    public static final String ERR_AUTHORIZATION = "nats: authorization violation";
    /**
     * This error message is defined as String {@value #ERR_NO_SERVERS}.
     */
    public static final String ERR_NO_SERVERS = "nats: no servers available for connection";
    /**
     * This error message is defined as String {@value #ERR_JSON_PARSE}.
     */
    public static final String ERR_JSON_PARSE = "nats: connect message, json parse err";
    /**
     * This error message is defined as String {@value #ERR_MAX_PAYLOAD}.
     */
    public static final String ERR_MAX_PAYLOAD = "nats: maximum payload exceeded";
    /**
     * This error message is defined as String {@value #ERR_MAX_MESSAGES}.
     */
    public static final String ERR_MAX_MESSAGES = "nats: maximum messages delivered";
    /**
     * This error message is defined as String {@value #ERR_SYNC_SUB_REQUIRED}.
     */
    public static final String ERR_SYNC_SUB_REQUIRED =
            "nats: illegal call on an async subscription";
    /**
     * This error message is defined as String {@value #ERR_NO_INFO_RECEIVED}.
     */
    public static final String ERR_NO_INFO_RECEIVED = "nats: protocol exception, INFO not received";
    /**
     * This error message is defined as String {@value #ERR_RECONNECT_BUF_EXCEEDED}.
     */
    public static final String ERR_RECONNECT_BUF_EXCEEDED = "nats: outbound buffer limit exceeded";
    /**
     * This error message is defined as String {@value #ERR_INVALID_CONNECTION}.
     */
    public static final String ERR_INVALID_CONNECTION = "nats: invalid connection";
    /**
     * This error message is defined as String {@value #ERR_INVALID_MESSAGE}.
     */
    public static final String ERR_INVALID_MESSAGE = "nats: invalid message or message nil";
    /**
     * This error message is defined as String {@value #ERR_STALE_CONNECTION}.
     */
    public static final String ERR_STALE_CONNECTION = "nats: " + Nats.STALE_CONNECTION;
    /**
     * This error message is defined as String {@value #ERR_PERMISSIONS_VIOLATION}.
     */
    public static final String ERR_PERMISSIONS_VIOLATION = "nats: " + Nats.PERMISSIONS_ERR;

    // jnats specific
    /**
     * This error message is defined as String {@value #ERR_CONNECTION_READ}.
     */
    public static final String ERR_CONNECTION_READ = "nats: connection read error";
    /**
     * This error message is defined as String {@value #ERR_PROTOCOL}.
     */
    public static final String ERR_PROTOCOL = "nats: protocol error";


    // Server error strings
    protected static final String SERVER_ERR_PARSER = "'Parser Error'";
    protected static final String SERVER_ERR_AUTH_TIMEOUT = "'Authorization Timeout'";
    protected static final String SERVER_ERR_AUTH_VIOLATION = "'Authorization Violation'";
    protected static final String SERVER_ERR_MAX_PAYLOAD = "'Maximum Payload Violation'";
    protected static final String SERVER_ERR_INVALID_SUBJECT = "'Invalid Subject'";
    protected static final String SERVER_UNKNOWN_PROTOCOL_OP = "'Unknown Protocol Operation'";
    protected static final String SERVER_ERR_TLS_REQUIRED = "'Secure Connection - TLS Required'";

    protected static final String NATS_SCHEME = "nats";
    protected static final String TCP_SCHEME = "tcp";
    protected static final String TLS_SCHEME = "tls";

    /**
     * Connection states for {@link Connection#getState()}.
     */
    public static enum ConnState {
        /**
         * The {@code Connection} is currently disconnected.
         */
        DISCONNECTED,
        /**
         * The {@code Connection} is currently connected.
         */
        CONNECTED,
        /**
         * The {@code Connection} is currently closed.
         */
        CLOSED,
        /**
         * The {@code Connection} is currently attempting to reconnect to a server it was previously
         * connected to.
         *
         * @see Connection#isReconnecting()
         */
        RECONNECTING,
        /**
         * The {@code Connection} is currently connecting to a server for the first time.
         */
        CONNECTING
    }




    /**
     * Creates a NATS connection.
     * 
     * @return the {@code Connection}
     * @throws IOException if a problem occurs
     * @throws TimeoutException if the connection attempt times out
     */
    public static Connection connect() throws IOException, TimeoutException {
        return defaultOptions().connect(DEFAULT_URL);
    }

    /**
     * Creates a NATS connection using the supplied URL list and default {@link Options}.
     * 
     * @param urls a comma-separated list of NATS server URLs
     * @return the {@code Connection}
     * @throws IOException if a problem occurs
     * @throws TimeoutException if the connection attempt times out
     */
    public static Connection connect(String urls) throws IOException, TimeoutException {
        return defaultOptions().connect(urls);
    }

    /**
     * Creates a NATS connection using the supplied URL list and {@code Options}.
     * 
     * @param urls a comma-separated list of NATS server URLs
     * @param options an {@link Options} object
     * @return the {@link Connection}
     * @throws IOException if a problem occurs
     * @throws TimeoutException if the connection attempt times out
     */
    public static Connection connect(String urls, Options options)
            throws IOException, TimeoutException {
        return options.connect(urls);
    }

    /**
     * Returns the default Options object.
     * 
     * @return the default {@link Options}
     */
    public static Options defaultOptions() {
        return new Options.Builder().build();
    }
}
