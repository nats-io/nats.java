// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client;

import java.util.List;
import java.util.Properties;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

import javax.net.ssl.SSLContext;

// TODO(sasbury): Support for local address (inetaddress)
// TODO(sasbury): SUpport for accumulate timeout
/**
 * The Options class specifies the connection options for a new NATs connection.
 * 
 * <p>
 * Options are created using a Builder. The builder supports chaining and will
 * create a default set of options if no methods are calls. The builder can also
 * be created from a properties object using the property names defined with the
 * prefix PROP_ in this class.
 * </p>
 */
public class Options {
    /**
     * Default server URL.
     *
     * <p>
     * This property is defined as {@value #DEFAULT_URL}
     */
    public static final String DEFAULT_URL = "nats://localhost:4222";

    /**
     * Default server port.
     *
     * <p>
     * This property is defined as {@value #DEFAULT_PORT}
     */
    public static final int DEFAULT_PORT = 4222;

    /**
     * Default maximum number of reconnect attempts.
     *
     * <p>
     * This property is defined as {@value #DEFAULT_MAX_RECONNECT}
     */
    public static final int DEFAULT_MAX_RECONNECT = 60;

    /**
     * Default wait time before attempting reconnection to the same server.
     *
     * <p>
     * This property is defined as 2 seconds.
     */
    public static final Duration DEFAULT_RECONNECT_WAIT = Duration.ofSeconds(2);

    /**
     * Default connection timeout.
     *
     * <p>
     * This property is defined as 2 seconds.}
     */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);

    /**
     * Default server ping interval. {@code <=0} means disabled.
     *
     * <p>
     * This property is defined as 2 minutes.
     */

    public static final Duration DEFAULT_PING_INTERVAL = Duration.ofMinutes(2);

    /**
     * Default interval to clean up cancelled/timedout requests.
     * A timer is used to clean up futures that were handed out but never completed
     * via a message.
     *
     * <p>
     * This property is defined as 5 seconds.
     */
    public static final Duration DEFAULT_REQUEST_CLEANUP_INTERVAL = Duration.ofSeconds(5);
    /**
     * Default maximum number of pings that have not received a response.
     *
     * <p>
     * This property is defined as {@value #DEFAULT_MAX_PINGS_OUT}
     */
    public static final int DEFAULT_MAX_PINGS_OUT = 2;

    /**
     * Default SSL protocol used to create an SSLContext if the {@link #PROP_SECURE
     * secure property} is used.
     */
    public static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";

    /**
     * Default of pending message buffer that is used for buffering messages that
     * are published during a disconnect/reconnect.
     *
     * <p>
     * This property is defined as {@value #DEFAULT_RECONNECT_BUF_SIZE} bytes, 8 *
     * 1024 * 1024.
     */
    public static final int DEFAULT_RECONNECT_BUF_SIZE = 8 * 1024 * 1024;

    static final String PFX = "io.nats.client.";

    /**
     * {@value #PROP_RECONNECTED_CB}, see
     * {@link Builder#reconnectHandler(ConnectionHandler) reconnectHandler}.
     */
    public static final String PROP_RECONNECTED_CB = PFX + "callback.reconnected";
    /**
     * {@value #PROP_DISCONNECTED_CB}, see
     * {@link Builder#disconnectHandler(ConnectionHandler) disconnectHandler}.
     */
    public static final String PROP_DISCONNECTED_CB = PFX + "callback.disconnected";
    /**
     * {@value #PROP_CLOSED_CB}, see {@link Builder#closeHandler(ConnectionHandler)
     * closeHandler}.
     */
    public static final String PROP_CLOSED_CB = PFX + "callback.closed";
    /**
     * {@value #PROP_EXCEPTION_HANDLER}, see
     * {@link Builder#errorHandler(ErrorHandler) errorHandler}.
     */
    public static final String PROP_EXCEPTION_HANDLER = PFX + "callback.exception";
    /**
     * {@value #PROP_MAX_PINGS}, see {@link Builder#maxPingsOut(int) maxPingsOut}.
     */
    public static final String PROP_MAX_PINGS = PFX + "maxpings";
    /**
     * {@value #PROP_PING_INTERVAL}, see {@link Builder#pingInterval(Duration)
     * pingInterval}.
     */
    public static final String PROP_PING_INTERVAL = PFX + "pinginterval";
    /**
     * {@value #PROP_CLEANUP_INTERVAL}, see {@link Builder#reqestCleanupInterval(Duration)
     * reqestCleanupInterval}.
     */
    public static final String PROP_CLEANUP_INTERVAL = PFX + "cleanupinterval";
    /**
     * {@value #PROP_CONNECTION_TIMEOUT}, see
     * {@link Builder#connectionTimeout(Duration) connectionTimeout}.
     */
    public static final String PROP_CONNECTION_TIMEOUT = PFX + "timeout";
    /**
     * {@value #PROP_RECONNECT_BUF_SIZE}, see
     * {@link Builder#reconnectBufferSize(long) reconnectBufferSize}.
     */
    public static final String PROP_RECONNECT_BUF_SIZE = PFX + "reconnect.buffer.size";
    /**
     * {@value #PROP_RECONNECT_WAIT}, see {@link Builder#reconnectWait(Duration)
     * reconnectWait}.
     */
    public static final String PROP_RECONNECT_WAIT = PFX + "reconnect.wait";
    /**
     * {@value #PROP_MAX_RECONNECT}, see {@link Builder#maxReconnects(int)
     * maxReconnects}.
     */
    public static final String PROP_MAX_RECONNECT = PFX + "reconnect.max";
    /**
     * {@value #PROP_TLS_DEBUG}, see {@link Builder#tlsDebug() tlsDebug}.
     */
    public static final String PROP_TLS_DEBUG = PFX + "tls.debug";
    /**
     * {@value #PROP_PEDANTIC}, see {@link Builder#pedantic() pedantic}.
     */
    public static final String PROP_PEDANTIC = PFX + "pedantic";
    /**
     * {@value #PROP_VERBOSE}, see {@link Builder#verbose() verbose}.
     */
    public static final String PROP_VERBOSE = PFX + "verbose";
    /**
     * {@value #PROP_CONNECTION_NAME}, see {@link Builder#connectionName(String)
     * connectionName}.
     */
    public static final String PROP_CONNECTION_NAME = PFX + "name";
    /**
     * {@value #PROP_NORANDOMIZE}, see {@link Builder#noRandomize() noRandomize}.
     */
    public static final String PROP_NORANDOMIZE = PFX + "norandomize";
    /**
     * {@value #PROP_SERVERS}, see {@link Builder#servers(String[]) servers}.
     */
    public static final String PROP_SERVERS = PFX + "servers";
    /**
     * {@value #PROP_PASSWORD}, see {@link Builder#userInfo(String, String)
     * userInfo}.
     */
    public static final String PROP_PASSWORD = PFX + "password";
    /**
     * {@value #PROP_USERNAME}, see {@link Builder#userInfo(String, String)
     * userInfo}.
     */
    public static final String PROP_USERNAME = PFX + "username";
    /**
     * {@value #PROP_TOKEN}, see {@link Builder#token(String) token}.
     */
    public static final String PROP_TOKEN = PFX + "token";
    /**
     * {@value #PROP_URL}, see {@link Builder#server(String) server}.
     */
    public static final String PROP_URL = PFX + "url";
    /**
     * {@value #PROP_SECURE}, see {@link Builder#sslContext(SSLContext) sslContext}.
     * 
     * This property is a boolean flag, but it tells the options parser to create a
     * default SSL context.
     * 
     * <p>
     * This properties is provided to support property based secure connections, for
     * more control, use properties to create the options and then override the
     * default your preferred SSL context.
     * </p>
     */
    public static final String PROP_SECURE = PFX + "secure";
    /**
     * {@value #PROP_USE_OLD_REQUEST_STYLE}, see {@link Builder#oldRequestStyle()
     * oldRequestStyle}.
     */
    public static final String PROP_USE_OLD_REQUEST_STYLE = "use.old.request.style";

    /**
     * Protocol key {@value #OPTION_VERBOSE}, see {@link Builder#verbose() verbose}.
     */
    public static final String OPTION_VERBOSE = "verbose";

    /**
     * Protocol key {@value #OPTION_PEDANTIC}, see {@link Builder#pedantic()
     * pedantic}.
     */
    public static final String OPTION_PEDANTIC = "pedantic";

    /**
     * Protocol key {@value #OPTION_SSL_REQUIRED}, see
     * {@link Builder#sslContext(SSLContext) sslContext}.
     */
    public static final String OPTION_SSL_REQUIRED = "ssl_required";

    /**
     * Protocol key {@value #OPTION_AUTH_TOKEN}, see {@link Builder#token(String)
     * token}.
     */
    public static final String OPTION_AUTH_TOKEN = "auth_token";

    /**
     * Protocol key {@value #OPTION_USER}, see
     * {@link Builder#userInfo(String, String) userInfo}.
     */
    public static final String OPTION_USER = "user";

    /**
     * Protocol key {@value #OPTION_PASSWORD}, see
     * {@link Builder#userInfo(String, String) userInfo}.
     */
    public static final String OPTION_PASSWORD = "password";

    /**
     * Protocol key {@value #OPTION_NAME}, see {@link Builder#connectionName(String)
     * connectionName}.
     */
    public static final String OPTION_NAME = "name";

    /**
     * Protocol key {@value #OPTION_LANG}, will be set to "Java".
     */
    public static final String OPTION_LANG = "lang";

    /**
     * Protocol key {@value #OPTION_VERSION}, will be set to
     * {@link Nats#CLIENT_VERSION CLIENT_VERSION}.
     */
    public static final String OPTION_VERSION = "version";

    /**
     * Protocol key {@value #OPTION_PROTOCOL}, will be set to 1.
     */
    public static final String OPTION_PROTOCOL = "protocol";

    private List<URI> servers;
    private final boolean noRandomize;
    private final String connectionName;
    private final boolean verbose;
    private final boolean pedantic;
    private final SSLContext sslContext;
    private final boolean tlsDebug;
    private final int maxReconnect;
    private final Duration reconnectWait;
    private final Duration connectionTimeout;
    private final Duration pingInterval;
    private final Duration requestCleanupInterval;
    private final int maxPingsOut;
    private final long reconnectBufferSize;
    private final String username;
    private final String password;
    private final String token;
    private final boolean useOldRequestStyle;

    private final ErrorHandler errorHandler;
    private final ConnectionHandler disconnectHandler;
    private final ConnectionHandler reconnectHandler;
    private final ConnectionHandler closeHandler;

    public static class Builder {

        private ArrayList<URI> servers = new ArrayList<URI>();
        private boolean noRandomize = false;
        private String connectionName = null;
        private boolean verbose = false;
        private boolean pedantic = false;
        private SSLContext sslContext = null;
        private boolean tlsDebug = false;
        private int maxReconnect = DEFAULT_MAX_RECONNECT;
        private Duration reconnectWait = DEFAULT_RECONNECT_WAIT;
        private Duration connectionTimeout = DEFAULT_TIMEOUT;
        private Duration pingInterval = DEFAULT_PING_INTERVAL;
        private Duration requestCleanupInterval = DEFAULT_REQUEST_CLEANUP_INTERVAL;
        private int maxPingsOut = DEFAULT_MAX_PINGS_OUT;
        private long reconnectBufferSize = DEFAULT_RECONNECT_BUF_SIZE;
        private String username = null;
        private String password = null;
        private String token = null;
        private boolean useOldRequestStyle = false;

        private ErrorHandler errorHandler = null;
        private ConnectionHandler disconnectHandler = null;
        private ConnectionHandler reconnectHandler = null;
        private ConnectionHandler closeHandler = null;

        /**
         * Constructs a new Builder with the default values.
         * 
         * <p>
         * One tiny correction is that the builder doesn't have a server url. When build
         * is called on a default builder it will add the {@link Options#DEFAULT_URL
         * default url} to its list of servers before creating the options object.
         * </p>
         */
        public Builder() {
        }

        /**
         * Constructs a new {@code Builder} from a {@link Properties} object.
         *
         * @param props
         *                  the {@link Properties} object
         */
        public Builder(Properties props) {
            if (props == null) {
                throw new IllegalArgumentException("Properties cannot be null");
            }

            if (props.containsKey(PROP_URL)) {
                this.server(props.getProperty(PROP_URL, DEFAULT_URL));
            }

            if (props.containsKey(PROP_USERNAME)) {
                this.username = props.getProperty(PROP_USERNAME, null);
            }

            if (props.containsKey(PROP_PASSWORD)) {
                this.password = props.getProperty(PROP_PASSWORD, null);
            }

            if (props.containsKey(PROP_TOKEN)) {
                this.token = props.getProperty(PROP_TOKEN, null);
            }

            if (props.containsKey(PROP_SERVERS)) {
                String str = props.getProperty(PROP_SERVERS);
                if (str.isEmpty()) {
                    throw new IllegalArgumentException(PROP_SERVERS + " cannot be empty");
                } else {
                    String[] servers = str.trim().split(",\\s*");
                    this.servers(servers);
                }
            }

            if (props.containsKey(PROP_NORANDOMIZE)) {
                this.noRandomize = Boolean.parseBoolean(props.getProperty(PROP_NORANDOMIZE));
            }

            if (props.containsKey(PROP_SECURE)) {
                boolean secure = Boolean.parseBoolean(props.getProperty(PROP_SECURE));

                if (secure) {
                    try {
                        this.sslContext = SSLContext.getInstance(DEFAULT_SSL_PROTOCOL);
                    } catch (NoSuchAlgorithmException nsae) {
                        throw new IllegalArgumentException("Unable to create ssl context from default protocol", nsae);
                    }
                }
            }

            if (props.containsKey(PROP_CONNECTION_NAME)) {
                this.connectionName = props.getProperty(PROP_CONNECTION_NAME, null);
            }

            if (props.containsKey(PROP_VERBOSE)) {
                this.verbose = Boolean.parseBoolean(props.getProperty(PROP_VERBOSE));
            }

            if (props.containsKey(PROP_PEDANTIC)) {
                this.pedantic = Boolean.parseBoolean(props.getProperty(PROP_PEDANTIC));
            }

            if (props.containsKey(PROP_TLS_DEBUG)) {
                this.tlsDebug = Boolean.parseBoolean(props.getProperty(PROP_TLS_DEBUG));
            }

            if (props.containsKey(PROP_MAX_RECONNECT)) {
                this.maxReconnect = Integer
                        .parseInt(props.getProperty(PROP_MAX_RECONNECT, Integer.toString(DEFAULT_MAX_RECONNECT)));
            }

            if (props.containsKey(PROP_RECONNECT_WAIT)) {
                int ms = Integer.parseInt(props.getProperty(PROP_RECONNECT_WAIT, "-1"));
                this.reconnectWait = (ms < 0) ? DEFAULT_RECONNECT_WAIT : Duration.ofMillis(ms);
            }

            if (props.containsKey(PROP_RECONNECT_BUF_SIZE)) {
                this.reconnectBufferSize = Long.parseLong(
                        props.getProperty(PROP_RECONNECT_BUF_SIZE, Long.toString(DEFAULT_RECONNECT_BUF_SIZE)));
            }

            if (props.containsKey(PROP_CONNECTION_TIMEOUT)) {
                int ms = Integer.parseInt(props.getProperty(PROP_CONNECTION_TIMEOUT, "-1"));
                this.connectionTimeout = (ms < 0) ? DEFAULT_TIMEOUT : Duration.ofMillis(ms);
            }

            if (props.containsKey(PROP_PING_INTERVAL)) {
                int ms = Integer.parseInt(props.getProperty(PROP_PING_INTERVAL, "-1"));
                this.pingInterval = (ms < 0) ? DEFAULT_PING_INTERVAL : Duration.ofMillis(ms);
            }

            if (props.containsKey(PROP_CLEANUP_INTERVAL)) {
                int ms = Integer.parseInt(props.getProperty(PROP_CLEANUP_INTERVAL, "-1"));
                this.requestCleanupInterval = (ms < 0) ? DEFAULT_REQUEST_CLEANUP_INTERVAL : Duration.ofMillis(ms);
            }

            if (props.containsKey(PROP_MAX_PINGS)) {
                this.maxPingsOut = Integer
                        .parseInt(props.getProperty(PROP_MAX_PINGS, Integer.toString(DEFAULT_MAX_PINGS_OUT)));
            }

            if (props.containsKey(PROP_USE_OLD_REQUEST_STYLE)) {
                this.useOldRequestStyle = Boolean.parseBoolean(props.getProperty(PROP_USE_OLD_REQUEST_STYLE));
            }

            if (props.containsKey(PROP_EXCEPTION_HANDLER)) {
                Object instance = createCallback(props.getProperty(PROP_EXCEPTION_HANDLER));
                this.errorHandler = (ErrorHandler) instance;
            }

            if (props.containsKey(PROP_CLOSED_CB)) {
                Object instance = createCallback(props.getProperty(PROP_CLOSED_CB));
                this.closeHandler = (ConnectionHandler) instance;
            }

            if (props.containsKey(PROP_DISCONNECTED_CB)) {
                Object instance = createCallback(props.getProperty(PROP_DISCONNECTED_CB));
                this.disconnectHandler = (ConnectionHandler) instance;
            }

            if (props.containsKey(PROP_RECONNECTED_CB)) {
                Object instance = createCallback(props.getProperty(PROP_RECONNECTED_CB));
                this.reconnectHandler = (ConnectionHandler) instance;
            }
        }

        private Object createCallback(String className) {
            Object instance = null;
            try {
                Class<?> clazz = Class.forName(className);
                Constructor<?> constructor = clazz.getConstructor();
                instance = constructor.newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException(e);
            }
            return instance;
        }

        /**
         * Add a server to the list of known servers.
         * 
         * @throws IllegalArgumentException
         *                                      if the url is not formatted correctly.
         */
        public Builder server(String serverURL) {
            try {
                this.servers.add(new URI(serverURL.trim()));
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Bad server URL: " + serverURL, e);
            }
            return this;
        }

        /**
         * Add an array of servers to the list of known servers.
         * 
         * @throws IllegalArgumentException
         *                                      if any url is not formatted correctly.
         */
        public Builder servers(String[] servers) {
            for (String s : servers) {
                if (s != null && !s.isEmpty()) {
                    try {
                        this.servers.add(new URI(s.trim()));
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("Bad server URL: " + s, e);
                    }
                }
            }
            return this;
        }

        /**
         * Turn on the old request style that uses a new inbox and subscriber for each
         * request.
         */
        public Builder oldRequestStyle() {
            this.useOldRequestStyle = true;
            return this;
        }

        /**
         * Turn off server pool randomization.
         */
        public Builder noRandomize() {
            this.noRandomize = true;
            return this;
        }

        /**
         * Set the connectionName.
         * 
         * @param name
         *                 the connections new name.
         */
        public Builder connectionName(String name) {
            this.connectionName = name;
            return this;
        }

        /**
         * Turn on verbose mode.
         */
        public Builder verbose() {
            this.verbose = true;
            return this;
        }

        /**
         * Turn on pedantic mode.
         */
        public Builder pedantic() {
            this.pedantic = true;
            return this;
        }

        /**
         * Set the SSL context, requires that the server supports TLS connections and
         * the URI specifies TLS.
         */
        public Builder sslContext(SSLContext ctx) {
            this.sslContext = ctx;
            return this;
        }

        /**
         * Enable tls debugging during the connect phase
         */
        public Builder tlsDebug() {
            this.tlsDebug = true;
            return this;
        }

        /**
         * Set the maximum number of reconnect attempts. Use 0 to turn off
         * auto-reconnect.
         */
        public Builder maxReconnects(int max) {
            this.maxReconnect = max;
            return this;
        }

        /**
         * Set the time to wait between reconnect attempts.
         */
        public Builder reconnectWait(Duration time) {
            this.reconnectWait = time;
            return this;
        }

        /**
         * Set the timeout for connection attempts.
         */
        public Builder connectionTimeout(Duration time) {
            this.connectionTimeout = time;
            return this;
        }

        /**
         * Set the interval between attempts to ping the server.
         */
        public Builder pingInterval(Duration time) {
            this.pingInterval = time;
            return this;
        }

        /**
         * Set the interval between cleaning passes on outstanding request futures.
         */
        public Builder requestCleanupInterval(Duration time) {
            this.requestCleanupInterval = time;
            return this;
        }

        /**
         * Set the maximum number of pings the client can have in flight.
         */
        public Builder maxPingsOut(int max) {
            this.maxPingsOut = max;
            return this;
        }

        /**
         * Set the maximum number of bytes to buffer in the client when trying to
         * reconnect.
         */
        public Builder reconnectBufferSize(long size) {
            this.reconnectBufferSize = size;
            return this;
        }

        /**
         * Set the username and password for basic authentication.
         */
        public Builder userInfo(String userName, String password) {
            this.username = userName;
            this.password = password;
            return this;
        }

        /**
         * Set the token for token-based authentication.
         */
        public Builder token(String token) {
            this.token = token;
            return this;
        }

        /**
         * Set the ErrorHandler to receive asyncrhonous error events related to this
         * connection.
         * 
         * @param handler
         *                    The new ErrorHandler for this connection.
         */
        public Builder errorHandler(ErrorHandler handler) {
            this.errorHandler = handler;
            return this;
        }

        /**
         * Set the ConnectionHandler to receive asyncrhonous notifications of disconnect
         * events.
         * 
         * @param handler
         *                    The new ConnectionHandler for this type of event.
         */
        public Builder disconnectHandler(ConnectionHandler handler) {
            this.disconnectHandler = handler;
            return this;
        }

        /**
         * Set the ConnectionHandler to receive asyncrhonous notifications of reconnect
         * events.
         * 
         * @param handler
         *                    The new ConnectionHandler for this type of event.
         */
        public Builder reconnectHandler(ConnectionHandler handler) {
            this.reconnectHandler = handler;
            return this;
        }

        /**
         * Set the ConnectionHandler to receive asyncrhonous notifications of close
         * events.
         * 
         * @param handler
         *                    The new ConnectionHandler for this type of event.
         */
        public Builder closeHandler(ConnectionHandler handler) {
            this.closeHandler = handler;
            return this;
        }

        public Options build() {
            if (servers.size() == 0) {
                server(DEFAULT_URL);
            }
            return new Options(this);
        }
    }

    private Options(Builder b) {
        this.servers = b.servers;
        this.noRandomize = b.noRandomize;
        this.connectionName = b.connectionName;
        this.verbose = b.verbose;
        this.pedantic = b.pedantic;
        this.sslContext = b.sslContext;
        this.tlsDebug = b.tlsDebug;
        this.maxReconnect = b.maxReconnect;
        this.reconnectWait = b.reconnectWait;
        this.connectionTimeout = b.connectionTimeout;
        this.pingInterval = b.pingInterval;
        this.requestCleanupInterval = b.requestCleanupInterval;
        this.maxPingsOut = b.maxPingsOut;
        this.reconnectBufferSize = b.reconnectBufferSize;
        this.username = b.username;
        this.password = b.password;
        this.token = b.token;
        this.useOldRequestStyle = b.useOldRequestStyle;

        this.errorHandler = b.errorHandler;
        this.disconnectHandler = b.disconnectHandler;
        this.reconnectHandler = b.reconnectHandler;
        this.closeHandler = b.closeHandler;
    }

    /**
     * @return The error handler, or null.
     */
    public ErrorHandler getErrorHandler() {
        return this.errorHandler;
    }

    /**
     * @return The disconnect handler, or null.
     */
    public ConnectionHandler getDisconnectHandler() {
        return this.disconnectHandler;
    }

    /**
     * @return The reconnect handler, or null.
     */
    public ConnectionHandler getCloseHandler() {
        return this.closeHandler;
    }

    /**
     * @return The reconnect handler, or null.
     */
    public ConnectionHandler getReconnectHandler() {
        return this.reconnectHandler;
    }

    /**
     * @return the servers stored in this options
     */
    public Collection<URI> getServers() {
        return servers;
    }

    /**
     * @return should we turn off randomization for server connection attempts
     */
    public boolean isNoRandomize() {
        return noRandomize;
    }

    /**
     * @return the connectionName
     */
    public String getConnectionName() {
        return connectionName;
    }

    /**
     * @return are we in verbose mode
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * @return are we using pedantic protocol
     */
    public boolean isPedantic() {
        return pedantic;
    }

    /**
     * @return the sslContext
     */
    public SSLContext getSslContext() {
        return sslContext;
    }

    /**
     * @return is tls debugging enabled
     */
    public boolean isTlsDebug() {
        return tlsDebug;
    }

    /**
     * @return the maxReconnect attempts to make before failing
     */
    public int getMaxReconnect() {
        return maxReconnect;
    }

    /**
     * @return the reconnectWait, used between reconnect attempts
     */
    public Duration getReconnectWait() {
        return reconnectWait;
    }

    /**
     * @return the connectionTimeout
     */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @return the pingInterval
     */
    public Duration getPingInterval() {
        return pingInterval;
    }

    /**
     * @return the request cleanup interval
     */
    public Duration getRequestCleanupInterval() {
        return requestCleanupInterval;
    }

    /**
     * @return the maxPingsOut to limit the number of pings on the wire
     */
    public int getMaxPingsOut() {
        return maxPingsOut;
    }

    /**
     * @return the reconnectBufferSize, to limit the amount of data held during
     *         reconnection attempts
     */
    public long getReconnectBufferSize() {
        return reconnectBufferSize;
    }

    /**
     * @return the username to use for basic authentication
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the password the password to use for basic authentication
     */
    public String getPassword() {
        return password;
    }

    /**
     * @return the token to be used for token-based authentication
     */
    public String getToken() {
        return token;
    }

    /**
     * @return the flag to turn on old style requests
     */
    public boolean isOldRequestStyle() {
        return useOldRequestStyle;
    }

    /**
     * Create the options string sent with a connect message.
     * 
     * @return the options String, basically JSON
     */
    public String buildProtocolConnectOptionsString(boolean includeAuth) {
        StringBuilder connectString = new StringBuilder();
        connectString.append("{");

        appendOption(connectString, Options.OPTION_LANG, Nats.CLIENT_LANGUAGE, true, false);
        appendOption(connectString, Options.OPTION_VERSION, Nats.CLIENT_VERSION, true, true);

        if (this.connectionName != null) {
            appendOption(connectString, Options.OPTION_NAME, this.connectionName, true, true);
        }

        appendOption(connectString, Options.OPTION_PROTOCOL, "1", false, true);

        appendOption(connectString, Options.OPTION_VERBOSE, String.valueOf(this.verbose), false, true);
        appendOption(connectString, Options.OPTION_PEDANTIC, String.valueOf(this.pedantic), false, true);
        appendOption(connectString, Options.OPTION_SSL_REQUIRED, String.valueOf(this.sslContext != null), false, true);

        if (includeAuth) {
            if (this.username != null) {
                appendOption(connectString, Options.OPTION_USER, this.username, true, true);
            }

            if (this.password != null) {
                appendOption(connectString, Options.OPTION_PASSWORD, this.password, true, true);
            }

            if (this.token != null) {
                appendOption(connectString, Options.OPTION_AUTH_TOKEN, this.token, true, true);
            }
        }

        connectString.append("}");
        return connectString.toString();
    }

    private void appendOption(StringBuilder builder, String key, String value, boolean quotes, boolean comma) {
        if (comma) {
            builder.append(",");
        }
        builder.append("\"");
        builder.append(key);
        builder.append("\"");
        builder.append(":");
        if (quotes) {
            builder.append("\"");
        }
        builder.append(value);
        if (quotes) {
            builder.append("\"");
        }
    }
}