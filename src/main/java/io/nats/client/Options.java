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

import io.nats.client.impl.DataPort;
import io.nats.client.impl.SSLUtils;
import io.nats.client.impl.SocketDataPort;

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
    // NOTE TO DEVS
    // To add an option, you have to:
    // * add property
    // * add field in builder
    // * add field in options
    // * addchainable method in builder
    // * add update build
    // * update constructor that takes properties
    // * optional default in statics

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

    /**
     * The default length, {@value #DEFAULT_MAX_CONTROL_LINE} bytes, the client will allow in an outgoing protocol control line.
     * 
     * <p>This value is configurable on the server, and should be set to match here.</p>
     */
    public static final int DEFAULT_MAX_CONTROL_LINE = 1024;

    /**
     * Default dataport class, which will use a TCP socket.
     */
    public static final String DEFAULT_DATA_PORT_TYPE = SocketDataPort.class.getCanonicalName();

    /**
     * Default size for buffers in the connection, not as available as other settings, this is primarily changed for testing.
     */
    public static final int DEFAULT_BUFFER_SIZE = 32 * 1024;

    static final String PFX = "io.nats.client.";

    /**
     * {@value #PROP_CONNECTION_CB}, see
     * {@link Builder#ConnectionListener(ConnectionListener) ConnectionListener}.
     */
    public static final String PROP_CONNECTION_CB = PFX + "callback.connection";
    /**
     * {@value #PROP_DATA_PORT_TYPE}, see
     * {@link Builder#dataPortType(String) dataPortType}.
     */
    public static final String PROP_DATA_PORT_TYPE = PFX + "dataport.type";
    /**
     * {@value #PROP_ERROR_LISTENER}, see
     * {@link Builder#ErrorListener(ErrorListener) ErrorListener}.
     */
    public static final String PROP_ERROR_LISTENER = PFX + "callback.error";
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
     * This property is a boolean flag, but it tells the options parser to use the
     * default SSL context. Set the default context before creating the options.
     */
    public static final String PROP_SECURE = PFX + "secure";
    /**
     * {@value #PROP_OPENTLS}, see {@link Builder#sslContext(SSLContext) sslContext}.
     * 
     * This property is a boolean flag, but it tells the options parser to use the
     * an SSL context that takes any server TLS certificate and does not provide
     * its own. The server must have tls_verify turned OFF for this option to work.
     */
    public static final String PROP_OPENTLS = PFX + "opentls";
    /**
     * {@value #PROP_USE_OLD_REQUEST_STYLE}, see {@link Builder#oldRequestStyle()
     * oldRequestStyle}.
     */
    public static final String PROP_USE_OLD_REQUEST_STYLE = "use.old.request.style";
    /**
     * {@value #PROP_MAX_CONTROL_LINE}, see {@link Builder#maxControlLine()
     * maxControlLine}.
     */
    public static final String PROP_MAX_CONTROL_LINE = "max.control.line";

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
     * Protocol key {@value #OPTION_TLS_REQUIRED}, see
     * {@link Builder#sslContext(SSLContext) sslContext}.
     */
    public static final String OPTION_TLS_REQUIRED = "tls_required";

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
    public static final String OPTION_PASSWORD = "pass";

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
    private final int maxReconnect;
    private final int maxControlLine;
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
    private final int bufferSize;

    private final ErrorListener errorListener;
    private final ConnectionListener connectionListener;
    private final String dataPortType;

    public static class Builder {

        private ArrayList<URI> servers = new ArrayList<URI>();
        private boolean noRandomize = false;
        private String connectionName = null;
        private boolean verbose = false;
        private boolean pedantic = false;
        private SSLContext sslContext = null;
        private int maxControlLine = DEFAULT_MAX_CONTROL_LINE;
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
        private int bufferSize = DEFAULT_BUFFER_SIZE;

        private ErrorListener errorListener = null;
        private ConnectionListener connectionListener = null;
        private String dataPortType = DEFAULT_DATA_PORT_TYPE;

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
         * If {@link Options#PROP_SECURE PROP_SECURE} is set, the builder will
         * try to use the default SSLContext {@link SSLContext#getDefault()}. If that
         * fails, no context is set and an IllegalArgumentException is thrown.
         *
         * @param props
         *                  the {@link Properties} object
         */
        public Builder(Properties props) throws IllegalArgumentException {
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
                        this.sslContext = SSLContext.getDefault();
                    } catch (NoSuchAlgorithmException e) {
                        this.sslContext = null;
                        throw new IllegalArgumentException("Unable to retrieve default SSL context");
                    }
                }
            }

            if (props.containsKey(PROP_OPENTLS)) {
                boolean tls = Boolean.parseBoolean(props.getProperty(PROP_OPENTLS));

                if (tls) {
                    try {
                        this.sslContext = SSLUtils.createOpenTLSContext();
                    } catch (Exception e) {
                        this.sslContext = null;
                        throw new IllegalArgumentException("Unable to create open SSL context");
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

            if (props.containsKey(PROP_MAX_CONTROL_LINE)) {
                int bytes = Integer.parseInt(props.getProperty(PROP_MAX_CONTROL_LINE, "-1"));
                this.maxControlLine = (bytes < 0) ? DEFAULT_MAX_CONTROL_LINE : bytes;
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

            if (props.containsKey(PROP_ERROR_LISTENER)) {
                Object instance = createInstanceOf(props.getProperty(PROP_ERROR_LISTENER));
                this.errorListener = (ErrorListener) instance;
            }

            if (props.containsKey(PROP_CONNECTION_CB)) {
                Object instance = createInstanceOf(props.getProperty(PROP_CONNECTION_CB));
                this.connectionListener = (ConnectionListener) instance;
            }

            if (props.containsKey(PROP_DATA_PORT_TYPE)) {
                this.dataPortType = props.getProperty(PROP_DATA_PORT_TYPE);
            }
        }

        static Object createInstanceOf(String className) {
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
         * @param serverURL the URL for the server ot add
         * @throws IllegalArgumentException if the url is not formatted correctly.
         * @return the Builder for chaining
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
         * @param servers A list of server URIs
         * @throws IllegalArgumentException if any url is not formatted correctly.
         * @return the Builder for chaining
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
         * @return the Builder for chaining
         */
        public Builder oldRequestStyle() {
            this.useOldRequestStyle = true;
            return this;
        }

        /**
         * Turn off server pool randomization.
         * @return the Builder for chaining
         */
        public Builder noRandomize() {
            this.noRandomize = true;
            return this;
        }

        /**
         * Set the connectionName.
         * 
         * @param name the connections new name.
         * @return the Builder for chaining
         */
        public Builder connectionName(String name) {
            this.connectionName = name;
            return this;
        }

        /**
         * Turn on verbose mode.
         * @return the Builder for chaining
         */
        public Builder verbose() {
            this.verbose = true;
            return this;
        }

        /**
         * Turn on pedantic mode.
         * @return the Builder for chaining
         */
        public Builder pedantic() {
            this.pedantic = true;
            return this;
        }

        /**
         * Set the SSL context to the {@link Options#DEFAULT_SSL_PROTOCOL default one}.
         * 
         * @throws NoSuchAlgorithmException If the default protocol is unavailable.
         * @throws IllegalArgumentException If there is no default SSL context.
         * @return the Builder for chaining
         */
        public Builder secure() throws NoSuchAlgorithmException, IllegalArgumentException {
            this.sslContext = SSLContext.getDefault();

            if(this.sslContext == null) {
                throw new IllegalArgumentException("No Default SSL Context");
            }
            return this;
        }

        /**
         * Set the SSL context to one that accespts any server certificate and has no client certificates.
         * 
         * @throws NoSuchAlgorithmException If the tls protocol is unavailable.
         * @return the Builder for chaining
         */
        public Builder opentls() throws NoSuchAlgorithmException {
            this.sslContext = SSLUtils.createOpenTLSContext();
            return this;
        }

        /**
         * Set the SSL context, requires that the server supports TLS connections and
         * the URI specifies TLS.
         * 
         * @param ctx the SSL Context to use for TLS connections
         * @return the Builder for chaining
         */
        public Builder sslContext(SSLContext ctx) {
            this.sslContext = ctx;
            return this;
        }

        /**
         * Equivalent to calling maxReconnects with 0.
         * @return the Builder for chaining
         */
        public Builder noReconnect() {
            this.maxReconnect = 0;
            return this;
        }

        /**
         * Set the maximum number of reconnect attempts. Use 0 to turn off
         * auto-reconnect.
         * 
         * @param max the maximum reconnect attempts
         * @return the Builder for chaining
         */
        public Builder maxReconnects(int max) {
            this.maxReconnect = max;
            return this;
        }

        /**
         * Set the time to wait between reconnect attempts to the same server.
         * 
         * @param time the time to wait
         * @return the Builder for chaining
         */
        public Builder reconnectWait(Duration time) {
            this.reconnectWait = time;
            return this;
        }

        /**
         * Set the maximum length of a control line sent by this connection.
         * 
         * @param bytes the max byte count
         * @return the Builder for chaining
         */
        public Builder maxControlLine(int bytes) {
            this.maxControlLine = bytes;
            return this;
        }

        /**
         * Set the timeout for connection attempts.
         * 
         * @param time the time to wait
         * @return the Builder for chaining
         */
        public Builder connectionTimeout(Duration time) {
            this.connectionTimeout = time;
            return this;
        }

        /**
         * Set the interval between attempts to ping the server.
         * 
         * @param the time between client to server pings
         * @return the Builder for chaining
         */
        public Builder pingInterval(Duration time) {
            this.pingInterval = time;
            return this;
        }

        /**
         * Set the interval between cleaning passes on outstanding request futures.
         * 
         * @param time the cleaning interval
         * @return the Builder for chaining
         */
        public Builder requestCleanupInterval(Duration time) {
            this.requestCleanupInterval = time;
            return this;
        }

        /**
         * Set the maximum number of pings the client can have in flight.
         * 
         * @param max the max pings
         * @return the Builder for chaining
         */
        public Builder maxPingsOut(int max) {
            this.maxPingsOut = max;
            return this;
        }

        /**
         * Sets the initial size for buffers in the conneciton, primarily for testing.
         */
        public Builder bufferSize(int size) {
            this.bufferSize = size;
            return this;
        }

        /**
         * Set the maximum number of bytes to buffer in the client when trying to
         * reconnect.
         * 
         * @param size the size in bytes
         * @return the Builder for chaining
         */
        public Builder reconnectBufferSize(long size) {
            this.reconnectBufferSize = size;
            return this;
        }

        /**
         * Set the username and password for basic authentication.
         * 
         * @param userName a non-empty user name
         * @param password the password, in plain text
         * @return the Builder for chaining
         */
        public Builder userInfo(String userName, String password) {
            this.username = userName;
            this.password = password;
            return this;
        }

        /**
         * Set the token for token-based authentication.
         * 
         * @param token The token
         * @return the Builder for chaining
         */
        public Builder token(String token) {
            this.token = token;
            return this;
        }

        /**
         * Set the ErrorListener to receive asyncrhonous error events related to this
         * connection.
         * 
         * @param listener The new ErrorListener for this connection.
         * @return the Builder for chaining
         */
        public Builder errorListener(ErrorListener listener) {
            this.errorListener = listener;
            return this;
        }

        /**
         * Set the ConnectionListener to receive asyncrhonous notifications of disconnect
         * events.
         * 
         * @param listener The new ConnectionListener for this type of event.
         * @return the Builder for chaining
         */
        public Builder connectionListener(ConnectionListener listener) {
            this.connectionListener = listener;
            return this;
        }

        /**
         * The class to use for this connections data port. This is an advanced setting
         * and primarily useful for testing.
         * 
         * @param dataPortClassName a valid and accesible class name
         * @return the Builder for chaining
         */
        public Builder dataPortType(String dataPortClassName) {
            this.dataPortType = dataPortClassName;
            return this;
        }

        /**
         * If the Options builder was not provided with a server, a default one will be included
         * {@link Options#DEFAULT_URL}. If only a single server URI is included, the builder
         * will try a few things to make connecting easier:
         *  * If there is no user/password is set but the URI has them, they will be used.
         *  * If there is no token is set but the URI has one, it will be used.
         *  * If the URI is of the form tls:// and no SSL context was assigned, the default one will
         *  be used {@link SSLContext#getDefault()}.
         *  * If the URI is of the form opentls:// and no SSL context was assigned one will be created
         * that does not check the servers certificate for validity. This is not secure and only provided
         * for tests and development.
         * 
         * @return the nwe options object
         * @throws IllegalStateException if there is a conflict in the options, like a token and a user/pass
         */
        public Options build() throws IllegalStateException {

            if (this.username != null && this.token != null) {
                throw new IllegalStateException("Options can't have token and username");
            }
            
            if (servers.size() == 0) {
                server(DEFAULT_URL);
            } else if (servers.size() == 1) { // Allow some URI based configs
                URI serverURI = servers.get(0);
                
                if (this.username==null && this.password==null && this.token == null) {
                    String userInfo = serverURI.getUserInfo();

                    if (userInfo != null) {
                        String[] info = userInfo.split(":");

                        if (info.length == 2) {
                            this.username = info[0];
                            this.password = info[1];
                        } else {
                            this.token = userInfo;
                        }
                    }
                }

                if ("tls".equals(serverURI.getScheme()) && this.sslContext == null)
                {
                    try {
                        this.sslContext = SSLContext.getDefault();
                    } catch (NoSuchAlgorithmException e) {
                        this.sslContext = null;
                    }
                } else if ("opentls".equals(serverURI.getScheme()) && this.sslContext == null)
                {
                    this.sslContext = SSLUtils.createOpenTLSContext();
                }
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
        this.maxControlLine = b.maxControlLine;
        this.bufferSize = b.bufferSize;

        this.errorListener = b.errorListener;
        this.connectionListener = b.connectionListener;
        this.dataPortType = b.dataPortType;
    }

    /**
     * @return The error listener, or null.
     */
    public ErrorListener getErrorListener() {
        return this.errorListener;
    }

    /**
     * @return The connection listener, or null.
     */
    public ConnectionListener getConnectionListener() {
        return this.connectionListener;
    }

    /**
     * @return Returns the dataport type this options will create.
     */
    public String getDataPortType() {
        return this.dataPortType;
    }

    /**
     * @return The data port described by these options, or the default type.
     */
    public DataPort buildDataPort() {
        return (DataPort) Options.Builder.createInstanceOf(dataPortType);
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
     * @return the maximum length of a control line
     */
    public int getMaxControlLine() {
        return maxControlLine;
    }

    /**
     * Checks if we have an ssl context, and if so returns true.
     * @return use tls
     */
    public boolean isTLSRequired() {
        return (this.sslContext != null);
    }

    /**
     * @return the sslContext
     */
    public SSLContext getSslContext() {
        return sslContext;
    }

    /**
     * @return the maxReconnect attempts to make before failing
     */
    public int getMaxReconnect() {
        return maxReconnect;
    }

    /**
     * @return the reconnectWait, used between reconnect attempts to the same server
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
     * @return the default size for buffers in the connection code
     */
    public int getBufferSize() {
        return bufferSize;
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

        appendOption(connectString, Options.OPTION_VERBOSE, String.valueOf(this.isVerbose()), false, true);
        appendOption(connectString, Options.OPTION_PEDANTIC, String.valueOf(this.isPedantic()), false, true);
        appendOption(connectString, Options.OPTION_TLS_REQUIRED, String.valueOf(this.isTLSRequired()), false, true);

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