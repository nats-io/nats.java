/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.Nats.DEFAULT_HOST;
import static io.nats.client.Nats.DEFAULT_PORT;
import static io.nats.client.Nats.DEFAULT_RECONNECT_BUF_SIZE;
import static io.nats.client.Nats.NATS_SCHEME;
import static io.nats.client.Nats.PROP_HOST;
import static io.nats.client.Nats.PROP_PORT;
import static io.nats.client.Nats.TCP_SCHEME;
import static io.nats.client.Nats.TLS_SCHEME;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLContext;

/**
 * A {@code ConnectionFactory} object encapsulates a set of connection configuration options. A
 * client uses it to create a connection to NATS.
 */
public class ConnectionFactory implements Cloneable {

    TcpConnectionFactory factory = null;
    URI url = null;
    String host = null;
    int port = -1;
    String username = null;
    String password = null;
    List<URI> servers = null;
    boolean noRandomize = false;
    String connectionName = null;
    boolean verbose = false;
    boolean pedantic = false;
    boolean secure = false;
    boolean reconnectAllowed = true;
    int maxReconnect = Nats.DEFAULT_MAX_RECONNECT;
    long reconnectWait = Nats.DEFAULT_RECONNECT_WAIT;
    int reconnectBufSize = DEFAULT_RECONNECT_BUF_SIZE;
    int connectionTimeout = Nats.DEFAULT_TIMEOUT;
    long pingInterval = Nats.DEFAULT_PING_INTERVAL;
    int maxPingsOut = Nats.DEFAULT_MAX_PINGS_OUT;
    SSLContext sslContext;
    ExceptionHandler exceptionHandler = null;
    ClosedCallback closedCallback;
    DisconnectedCallback disconnectedCallback;
    ReconnectedCallback reconnectedCallback;
    String urlString = null;
    boolean tlsDebug;

    /**
     * Constructs a new connection factory from a {@link Properties} object.
     *
     * @param props the {@link Properties} object
     */
    public ConnectionFactory(Properties props) {
        Options opts = new Options.Builder(props).build();

        // Get the ConnectionFactory-specific options
        // PROP_HOST
        if (props.containsKey(PROP_HOST)) {
            this.host = props.getProperty(PROP_HOST, DEFAULT_HOST);
        }
        // PROP_PORT
        if (props.containsKey(PROP_PORT)) {
            this.port =
                    Integer.parseInt(props.getProperty(PROP_PORT, Integer.toString(DEFAULT_PORT)));
        }

        this.urlString = opts.url;
        this.url = URI.create(opts.url);
        this.username = opts.username;
        this.password = opts.password;
        if (opts.servers != null) {
            this.servers = new ArrayList<URI>(opts.servers);
        }
        this.noRandomize = opts.noRandomize;
        this.connectionName = opts.connectionName;
        this.verbose = opts.verbose;
        this.pedantic = opts.pedantic;
        this.secure = opts.secure;
        this.reconnectAllowed = opts.allowReconnect;
        this.maxReconnect = opts.maxReconnect;
        this.reconnectBufSize = opts.reconnectBufSize;
        this.reconnectWait = opts.reconnectWait;
        this.connectionTimeout = opts.connectionTimeout;
        this.pingInterval = opts.pingInterval;
        this.maxPingsOut = opts.maxPingsOut;
        this.sslContext = opts.sslContext;
        this.exceptionHandler = opts.asyncErrorCb;
        this.closedCallback = opts.closedCb;
        this.disconnectedCallback = opts.disconnectedCb;
        this.reconnectedCallback = opts.reconnectedCb;
        this.factory = opts.factory;
    }

    /**
     * Constructs a connection factory using default parameters.
     */
    public ConnectionFactory() {
        this(null, null);
    }

    /**
     * Constructs a connection factory using the supplied URL string as default.
     *
     * @param url the default server URL to use
     */
    public ConnectionFactory(String url) {
        this(url, null);
    }

    /**
     * Constructs a connection factory from a list of NATS server URL strings.
     *
     * @param servers the list of cluster server URL strings
     */
    public ConnectionFactory(String[] servers) {
        this(null, servers);
    }

    /**
     * Constructs a connection factory from a list of NATS server URLs, using {@code url} as the
     * primary address.
     * <p>
     * <p>If {@code url} contains a single server address, that address will be first in the server
     * list, even if {@link #isNoRandomize()} is {@code false}.
     * <p>
     * <p>If {@code url} is a comma-delimited list of servers, then {@code servers} will be ignored.
     *
     * @param url     the default server URL to set
     * @param servers the list of cluster server URL strings
     */
    public ConnectionFactory(String url, String[] servers) {
        if (url != null && url.contains(",")) {
            this.setServers(url);
        } else {
            this.setUrl(url);
            this.setServers(servers);
        }
    }

    /**
     * Constructs a {@code ConnectionFactory} by copying the supplied {@code ConnectionFactory}.
     *
     * @param cf the {@code ConnectionFactory} to copy
     */
    public ConnectionFactory(ConnectionFactory cf) {
        this.factory = cf.factory;
        this.url = cf.url;
        this.host = cf.host;
        this.port = cf.port;
        this.username = cf.username;
        this.password = cf.password;
        if (cf.servers != null) {
            this.servers = new ArrayList<URI>(cf.servers);
        }
        this.noRandomize = cf.noRandomize;
        this.connectionName = cf.connectionName;
        this.verbose = cf.verbose;
        this.pedantic = cf.pedantic;
        this.secure = cf.secure;
        this.reconnectAllowed = cf.reconnectAllowed;
        this.maxReconnect = cf.maxReconnect;
        this.reconnectBufSize = cf.reconnectBufSize;
        this.reconnectWait = cf.reconnectWait;
        this.connectionTimeout = cf.connectionTimeout;
        this.pingInterval = cf.pingInterval;
        this.maxPingsOut = cf.maxPingsOut;
        this.sslContext = cf.sslContext;
        this.exceptionHandler = cf.exceptionHandler;
        this.closedCallback = cf.closedCallback;
        this.disconnectedCallback = cf.disconnectedCallback;
        this.reconnectedCallback = cf.reconnectedCallback;
        this.urlString = cf.urlString;
        this.tlsDebug = cf.tlsDebug;
    }

    /**
     * Creates an active connection to a NATS server
     *
     * @return the Connection.
     * @throws IOException      if a Connection cannot be established for some reason.
     * @throws TimeoutException if the connection timeout has been exceeded.
     */
    public Connection createConnection() throws IOException, TimeoutException {
        return new ConnectionImpl(options()).connect();
    }

    protected URI constructUri() {
        URI res = null;
        if (url != null) {
            res = url;
        } else {
            String str;
            if (getHost() != null) {
                str = "nats://";
                if (getUsername() != null) {
                    str = str.concat(getUsername());
                    if (getPassword() != null) {
                        str = str.concat(":" + getPassword());
                    }
                    str = str.concat("@");
                }
                str = str.concat(getHost() + ":");
                if (getPort() > -1) {
                    str = str.concat(String.valueOf(getPort()));
                } else {
                    str = str.concat(String.valueOf(DEFAULT_PORT));
                }
                res = URI.create(str);
            }
        }
        return res;
    }

    protected Options options() {
        String urlString = null;
        if (url != null) {
            url = constructUri();
            urlString = url.toString();
        }

        Options.Builder result =
                new Options.Builder().userInfo(username, password);

        result.url = urlString;
        result.servers = servers;

        if (noRandomize) {
            result = result.dontRandomize();
        }
        if (verbose) {
            result = result.verbose();
        }
        if (pedantic) {
            result = result.pedantic();
        }
        if (secure) {
            result = result.secure();
        }
        if (tlsDebug) {
            result = result.tlsDebug();
        }
        if (!reconnectAllowed) {
            result = result.noReconnect();
        }

        result = result.factory(factory).maxReconnect(maxReconnect).reconnectWait(reconnectWait)
                .reconnectBufSize(reconnectBufSize).name(connectionName).timeout(connectionTimeout)
                .pingInterval(pingInterval).maxPingsOut(maxPingsOut).sslContext(sslContext)
                .closedCb(closedCallback).disconnectedCb(disconnectedCallback)
                .reconnectedCb(reconnectedCallback).errorCb(exceptionHandler);
        return result.build();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public ConnectionFactory clone() {
        return new ConnectionFactory(this);
    }

    /**
     * Convenience function to set host, port, username, password from a java.net.URI. Any omitted
     * URI elements are left unchanged in the corresponding fields.
     *
     * @param uri the URI to set
     */
    public void setUri(URI uri) {
        this.url = uri;

        String scheme = uri.getScheme().toLowerCase();
        if (!(NATS_SCHEME.equals(scheme) || TCP_SCHEME.equals(scheme)
                || TLS_SCHEME.equals(scheme))) {
            throw new IllegalArgumentException("Wrong scheme in NATS URI: " + uri.getScheme());
        }
        String host = uri.getHost();
        if (host != null) {
            setHost(host);
        }

        int port = uri.getPort();
        if (port != -1) {
            setPort(port);
        }

        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            String[] userpass = userInfo.split(":");
            if (userpass[0].length() > 0) {
                setUsername(userpass[0]);
                switch (userpass.length) {
                    case 1:
                        break;
                    case 2:
                        setPassword(userpass[1]);
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Bad user info in NATS " + "URI: " + userInfo);
                }
            }
        }
    }

    /**
     * Returns the default TCP connection factory, which will be used to create TCP connections to
     * the NATS server.
     *
     * @return the TCP connection factory
     */
    TcpConnectionFactory getTcpConnectionFactory() {
        return this.factory;
    }

    /**
     * Sets the default TCP connection factory, which will be used to create TCP connections to the
     * NATS server.
     *
     * @param factory the {@code TcpConnectionFactory} to set
     */
    void setTcpConnectionFactory(TcpConnectionFactory factory) {
        this.factory = factory;
    }

    /**
     * Returns the default server URL string, if set.
     *
     * @return the default server URL, or {@code null} if not set
     */
    public String getUrlString() {
        return this.urlString;
    }

    /**
     * Sets the default server URL string.
     * <p>
     * <p>If {@code url} is a comma-delimited list, then {@link #setServers(String)} will be invoked
     * without setting the default server URL string.
     *
     * @param url the URL to set
     */
    public void setUrl(String url) {
        if (url == null) {
            this.url = null;
        } else if (url.contains(",")) {
            setServers(url);
        } else {
            try {
                this.urlString = url;
                this.setUri(new URI(urlString));
            } catch (NullPointerException | URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    /**
     * Gets the default server host, if set.
     *
     * @return the host, or {@code null} if not set
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Sets the default server host.
     *
     * @param host the host to set
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Gets the default server port, if set.
     *
     * @return the default server port, or {@code -1} if not set
     */
    public int getPort() {
        return this.port;
    }

    /**
     * Sets the default server port.
     *
     * @param port the port to set
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets the default username, if set.
     *
     * @return the username, or {@code null} if not set
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * Sets the default username.
     *
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Gets the default password, or {@code null} if not set.
     *
     * @return the password
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * Sets the default password.
     *
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Gets the server list as {@code URI}.
     *
     * @return the list of server {@code URI}s, or {@code null} if not set
     */
    public List<URI> getServers() {
        return this.servers;
    }

    /**
     * Sets the server list from a list of {@code URI}.
     *
     * @param servers the servers to set
     */
    public void setServers(List<URI> servers) {
        this.servers = servers;
    }

    /**
     * Sets the server list from a comma-delimited list of server addresses in a single string.
     *
     * @param urlString the servers to set
     */
    public void setServers(String urlString) {
        String[] servers = urlString.trim().split("\\s*,\\s*");
        this.setServers(servers);
    }

    /**
     * Sets the server list from a list of {@code String}.
     *
     * @param servers the servers to set
     * @throws IllegalArgumentException if any of the {@code URI}s are malformed
     */
    public void setServers(String[] servers) {
        if (servers == null) {
            this.servers = null;
        } else {
            if (this.servers == null) {
                this.servers = new ArrayList<URI>();
            }
            this.servers.clear();
            for (String s : servers) {
                try {
                    this.servers.add(new URI(s.trim()));
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        }
    }

    /**
     * Indicates whether server list randomization is disabled.
     * <p>
     * <p>{@code true} means that the server list will be traversed in the order in which it was
     * received
     * <p>
     * <p>{@code false} means that the server list will be randomized before it is traversed
     *
     * @return {@code true} if server list randomization is disabled, otherwise {@code false}
     */
    public boolean isNoRandomize() {
        return noRandomize;
    }

    /**
     * Disables or enables server list randomization.
     *
     * @param noRandomize the noRandomize to set
     */
    public void setNoRandomize(boolean noRandomize) {
        this.noRandomize = noRandomize;
    }

    /**
     * Gets the name associated with this Connection
     *
     * @return the name associated with this Connection.
     */
    public String getConnectionName() {
        return this.connectionName;
    }

    /**
     * Sets the name associated with this Connection
     *
     * @param connectionName the name to set.
     */
    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    /**
     * Indicates whether {@code verbose} is set.
     * <p>
     * <p>When {@code verbose==true}, the server will acknowledge each protocol line with
     * {@code +OK or -ERR}
     *
     * @return whether {@code verbose} is set
     */
    public boolean isVerbose() {
        return this.verbose;
    }

    /**
     * Sets whether {@code verbose} is set.
     *
     * @param verbose whether or not this connection should require protocol acks from the server
     *                (+OK/-ERR)
     */
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * Indicates whether strict server-side protocol checking is enabled.
     *
     * @return whether {@code pedantic} is set
     */
    public boolean isPedantic() {
        return this.pedantic;
    }

    /**
     * Sets whether strict server-side protocol checking is enabled.
     * <p>
     * <p>When {@code pedantic==true} , strict server-side protocol checking occurs.
     *
     * @param pedantic whether or not this connection should require strict server-side protocol
     *                 checking
     */
    public void setPedantic(boolean pedantic) {
        this.pedantic = pedantic;
    }

    /**
     * Indicates whether to require a secure connection with the NATS server.
     *
     * @return {@code true} if secure is required, otherwise {@code false}
     */
    public boolean isSecure() {
        return this.secure;
    }

    /**
     * Sets whether to require a secure connection with the NATS server.
     *
     * @param secure whether to require a secure connection with the NATS server
     */
    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    /**
     * Indicates whether TLS debug output should be enabled.
     *
     * @return {@code true} if TLS debug is enabled, otherwise {@code false}
     */
    public boolean isTlsDebug() {
        return tlsDebug;
    }

    /**
     * Sets whether TLS debug output should be enabled.
     *
     * @param debug whether TLS debug output should be enabled
     */
    public void setTlsDebug(boolean debug) {
        this.tlsDebug = debug;
    }

    /**
     * Indicates whether reconnection is enabled.
     *
     * @return {@code true} if reconnection is allowed, otherwise {@code false}
     */
    public boolean isReconnectAllowed() {
        return this.reconnectAllowed;
    }

    /**
     * Sets whether reconnection is enabled.
     *
     * @param reconnectAllowed whether to allow reconnects
     */
    public void setReconnectAllowed(boolean reconnectAllowed) {
        this.reconnectAllowed = reconnectAllowed;
    }

    /**
     * Gets the maximum number of reconnection attempts for this connection.
     *
     * @return the maximum number of reconnection attempts
     */
    public int getMaxReconnect() {
        return this.maxReconnect;
    }

    /**
     * Sets the maximum number of reconnection attempts for this connection.
     *
     * @param max the maximum number of reconnection attempts
     */
    public void setMaxReconnect(int max) {
        this.maxReconnect = max;
    }

    /**
     * Returns the reconnect wait interval in milliseconds. This is the amount of time to wait
     * before attempting reconnection to the current server
     *
     * @return the reconnect wait interval in milliseconds
     */
    public long getReconnectWait() {
        return this.reconnectWait;
    }

    /**
     * Sets the maximum size in bytes of the pending message buffer, which is used to buffer
     * messages between a disconnect and subsequent reconnect.
     *
     * @param size the reconnect buffer size, in bytes
     */
    public void setReconnectBufSize(int size) {
        if (size <= 0) {
            this.reconnectBufSize = DEFAULT_RECONNECT_BUF_SIZE;
        } else {
            this.reconnectBufSize = size;
        }
    }

    /**
     * Returns the maximum size in bytes of the pending message buffer, which is used to buffer
     * messages between a disconnect and subsequent reconnect.
     *
     * @return the reconnect buffer size, in bytes
     */
    public long getReconnectBufSize() {
        return this.reconnectBufSize;
    }

    /**
     * Sets the reconnect wait interval in milliseconds. This is the amount of time to wait before
     * attempting reconnection to the current server
     *
     * @param interval the reconnectWait to set
     */
    public void setReconnectWait(long interval) {
        this.reconnectWait = interval;
    }

    /**
     * Returns the connection timeout interval in milliseconds. This is the maximum amount of time
     * to wait for a connection to a NATS server to complete successfully
     *
     * @return the connection timeout
     */
    public int getConnectionTimeout() {
        return this.connectionTimeout;
    }

    /**
     * Sets the connection timeout interval in milliseconds. This is the maximum amount of time to
     * wait for a connection to a NATS server to complete successfully
     *
     * @param timeout the connection timeout
     * @throws IllegalArgumentException if {@code timeout < 0}
     */
    public void setConnectionTimeout(int timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException("TCP connection timeout cannot be negative");
        }
        this.connectionTimeout = timeout;
    }

    /**
     * Gets the server ping interval in milliseconds. The connection will send a PING to the server
     * at this interval to ensure the server is still alive
     *
     * @return the pingInterval
     */
    public long getPingInterval() {
        return this.pingInterval;
    }

    /**
     * Sets the server ping interval in milliseconds. The connection will send a PING to the server
     * at this interval to ensure the server is still alive
     *
     * @param interval the ping interval to set in milliseconds
     */
    public void setPingInterval(long interval) {
        this.pingInterval = interval;
    }

    /**
     * Returns the maximum number of outstanding server pings
     *
     * @return the maximum number of oustanding outbound pings before marking the Connection stale
     * and triggering reconnection (if allowed).
     */
    public int getMaxPingsOut() {
        return this.maxPingsOut;
    }

    /**
     * Sets the maximum number of outstanding pings (pings for which no pong has been received).
     * Once this limit is exceeded, the connection is marked as stale and closed.
     *
     * @param max the maximum number of outstanding pings
     */
    public void setMaxPingsOut(int max) {
        this.maxPingsOut = max;
    }

    /**
     * Returns the {@link ClosedCallback}, if one is registered.
     *
     * @return the {@link ClosedCallback}, if one is registered
     */
    public ClosedCallback getClosedCallback() {
        return closedCallback;
    }

    /**
     * Sets the {@link ClosedCallback}.
     *
     * @param cb the {@link ClosedCallback} to set
     */
    public void setClosedCallback(ClosedCallback cb) {
        this.closedCallback = cb;
    }

    /**
     * Returns the {@link DisconnectedCallback}, if one is registered.
     *
     * @return the {@link DisconnectedCallback}, if one is registered
     */
    public DisconnectedCallback getDisconnectedCallback() {
        return disconnectedCallback;
    }

    /**
     * Sets the {@link DisconnectedCallback}.
     *
     * @param cb the {@link DisconnectedCallback} to set
     */
    public void setDisconnectedCallback(DisconnectedCallback cb) {
        this.disconnectedCallback = cb;
    }

    /**
     * Returns the {@link ReconnectedCallback}, if one is registered.
     *
     * @return the {@link ReconnectedCallback}, if one is registered
     */
    public ReconnectedCallback getReconnectedCallback() {
        return reconnectedCallback;
    }

    /**
     * Sets the {@link ReconnectedCallback}.
     *
     * @param cb the {@link ReconnectedCallback} to set
     */
    public void setReconnectedCallback(ReconnectedCallback cb) {
        this.reconnectedCallback = cb;
    }

    /**
     * Returns the {@link ExceptionHandler}, if one is registered.
     *
     * @return the {@link ExceptionHandler}, if one is registered
     */
    public ExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    /**
     * Sets the {@link ExceptionHandler}.
     *
     * @param exceptionHandler the {@link ExceptionHandler} to set for connection.
     */
    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        if (exceptionHandler == null) {
            throw new IllegalArgumentException("ExceptionHandler cannot be null!");
        }
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Returns the {@link SSLContext} for this connection factory.
     *
     * @return the {@link SSLContext} for this connection factory
     * @deprecated use {@link #getSSLContext} instead.
     */
    @Deprecated
    public SSLContext getSslContext() {
        return sslContext;
    }

    /**
     * Returns the {@link SSLContext} for this connection factory.
     *
     * @return the {@link SSLContext} for this connection factory
     */
    public SSLContext getSSLContext() {
        return sslContext;
    }

    /**
     * Sets the {@link SSLContext} for this connection factory.
     *
     * @param ctx the {@link SSLContext} to set
     * @deprecated use {@link #setSSLContext} instead
     */
    @Deprecated
    public void setSslContext(SSLContext ctx) {
        setSSLContext(ctx);
    }

    /**
     * Sets the {@link SSLContext} for this connection factory.
     *
     * @param ctx the {@link SSLContext} to set
     */
    public void setSSLContext(SSLContext ctx) {
        this.sslContext = ctx;
    }
}
