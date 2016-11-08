/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLContext;

import static io.nats.client.Nats.*;
import static io.nats.client.Nats.DEFAULT_MAX_PINGS_OUT;

/**
 * An {@code Options} object contains the immutable (and some mutable) configuration settings for a {@link Connection}.
 *
 * <p>The {@code Options} object is constructed using methods of an {@link Options.Builder} as in the following
 * example:
 *
 * <pre>
 *    Options opts = new Options.Builder()
 *            .noReconnect()
 *            .timeout(3, TimeUnit.SECONDS)
 *            .build();
 *
 *    // Now connect
 *    Connection nc = Nats.connect("nats://example.company.com:4222", opts);
 *
 * </pre>
 */
public class Options {
    protected String url;
    List<URI> servers;
    boolean noRandomize;
    String connectionName;
    boolean verbose;
    boolean pedantic;
    boolean secure;
    SSLContext sslContext;
    // Print console output during connection handshake (java-specific)
    boolean tlsDebug;
    boolean allowReconnect;
    int maxReconnect;
    long reconnectWait;
    int connectionTimeout;
    long pingInterval;
    int maxPingsOut;
    // Connection handlers
    public ClosedCallback closedCb;
    public DisconnectedCallback disconnectedCb;
    public ReconnectedCallback reconnectedCb;
    public ExceptionHandler asyncErrorCb;

    // Size of the backing ByteArrayOutputStream buffer during reconnect.
    // Once this has been exhausted publish operations will error.
    int reconnectBufSize;

    String username;
    String password;
    String token;

    // TODO Allow users to set a custom "dialer" like Go. For now keep package-private
    TcpConnectionFactory factory;

    // private List<X509Certificate> certificates =
    // new ArrayList<X509Certificate>();

    private Options(Builder builder) {
        this.factory = builder.factory;
        this.url = builder.url;
        this.username = builder.username;
        this.password = builder.password;
        this.token = builder.token;
        if (builder.servers != null) {
            this.servers = new ArrayList<URI>(builder.servers);
        }
        this.noRandomize = builder.noRandomize;
        this.connectionName = builder.connectionName;
        this.verbose = builder.verbose;
        this.pedantic = builder.pedantic;
        this.secure = builder.secure;
        this.allowReconnect = builder.allowReconnect;
        this.maxReconnect = builder.maxReconnect;
        this.reconnectBufSize = builder.reconnectBufSize;
        this.reconnectWait = builder.reconnectWait;
        this.connectionTimeout = builder.connectionTimeout;
        this.pingInterval = builder.pingInterval;
        this.maxPingsOut = builder.maxPingsOut;
        this.sslContext = builder.sslContext;
        this.tlsDebug = builder.tlsDebug;
        this.disconnectedCb = builder.disconnectedCb;
        this.closedCb = builder.closedCb;
        this.reconnectedCb = builder.reconnectedCb;
        this.asyncErrorCb = builder.asyncErrorCb;

    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof Options)) {
            return false;
        }

        Options other = (Options) obj;

        return (compare(url, other.url)
        && compare(username, other.username)
        && compare(password, other.password)
        && compare(token, other.token)
        && compare(servers, other.servers)
        && Boolean.compare(noRandomize, other.noRandomize) == 0
        && compare(connectionName, other.connectionName)
        && Boolean.compare(verbose, other.verbose) == 0
        && Boolean.compare(pedantic, other.pedantic) == 0
        && Boolean.compare(secure, other.secure) == 0
        && Boolean.compare(allowReconnect, other.allowReconnect) ==0
        && Integer.compare(maxReconnect, other.maxReconnect) == 0
        && Integer.compare(reconnectBufSize, other.reconnectBufSize) == 0
        && Long.compare(reconnectWait, other.reconnectWait) == 0
        && Integer.compare(connectionTimeout, other.connectionTimeout) == 0
        && Long.compare(pingInterval, other.pingInterval) == 0
        && Integer.compare(maxPingsOut, other.maxPingsOut) == 0
        && (sslContext == null ? other.sslContext == null : sslContext.equals(other.sslContext))
        && Boolean.compare(tlsDebug, other.tlsDebug) == 0
        && (factory == null ? other.factory == null : factory == other.factory)
        && (disconnectedCb == null ? other.disconnectedCb == null : disconnectedCb == other.disconnectedCb)
        && (closedCb == null ? other.closedCb == null : closedCb == other.closedCb)
        && (reconnectedCb == null ? other.reconnectedCb == null : reconnectedCb == other.reconnectedCb)
        && (asyncErrorCb == null ? other.asyncErrorCb == null : asyncErrorCb == other.asyncErrorCb));
    }

    static boolean compare(String str1, String str2) {
        return (str1 == null ? str2 == null : str1.equals(str2));
    }

    static boolean compare(List<URI> first, List<URI> second) {
        if (first == null || second == null) {
            return (first == null && second == null);
        }
        if (first.size() != second.size()) {
            return false;
        }
        for (int i = 0; i < first.size(); i++) {
            URI left = first.get(i);
            URI right = second.get(i);
            if (!(left == null ? right == null : left.equals(right))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a connection using this {@code Options} object.
     *
     * @return the {@code Connection}
     * @throws IOException if something goes wrong
     * @throws TimeoutException if the connection doesn't complete within the configured timeout
     */
    public Connection connect() throws IOException, TimeoutException {
        return new ConnectionImpl(this).connect();
    }

    public TcpConnectionFactory getFactory() {
        return factory;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getToken() {
        return this.token;
    }

    public List<URI> getServers() {
        return servers;
    }

    public boolean isNoRandomize() {
        return noRandomize;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public boolean isPedantic() {
        return pedantic;
    }

    public boolean isSecure() {
        return secure;
    }

    public boolean isTlsDebug() {
        return tlsDebug;
    }

    public boolean isReconnectAllowed() {
        return allowReconnect;
    }

    public int getMaxReconnect() {
        return maxReconnect;
    }

    public int getReconnectBufSize() {
        return reconnectBufSize;
    }

    public long getReconnectWait() {
        return reconnectWait;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public long getPingInterval() {
        return pingInterval;
    }

    public int getMaxPingsOut() {
        return maxPingsOut;
    }

    public ExceptionHandler getExceptionHandler() {
        return asyncErrorCb;
    }

    public ClosedCallback getClosedCallback() {
        return closedCb;
    }

    public ReconnectedCallback getReconnectedCallback() {
        return reconnectedCb;
    }

    public DisconnectedCallback getDisconnectedCallback() {
        return disconnectedCb;
    }

    // public void addCertificate(X509Certificate cert) {
    // if (cert==null)
    // throw new IllegalArgumentException("Null certificate");
    // certificates.add(cert);
    // }
    //
    // public void addCertificate(byte[] cert) throws CertificateException {
    // if (cert==null)
    // throw new IllegalArgumentException("Null certificate");
    // CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
    // InputStream in = new ByteArrayInputStream(cert);
    // X509Certificate theCert = (X509Certificate)certFactory.generateCertificate(in);
    // certificates.add(theCert);
    // }
    //
    // public void addCertificate(String cert) throws CertificateException {
    // addCertificate(cert.getBytes(Charset.forName("UTF-8")));
    // }

    public SSLContext getSslContext() {
        return sslContext;
    }

    /**
     * An {@link Options} builder.
     */
    public static final class Builder {
        String url;
        List<URI> servers;
        private String username;
        private String password;
        private String token;
        private boolean noRandomize;
        private String connectionName;
        private boolean verbose;
        private boolean pedantic;
        private boolean secure;
        private boolean allowReconnect = true;
        private int maxReconnect = Nats.DEFAULT_MAX_RECONNECT;
        private int reconnectBufSize = Nats.DEFAULT_RECONNECT_BUF_SIZE;
        private long reconnectWait = Nats.DEFAULT_RECONNECT_WAIT;
        private int connectionTimeout = Nats.DEFAULT_TIMEOUT;
        private long pingInterval = Nats.DEFAULT_PING_INTERVAL;
        private int maxPingsOut = Nats.DEFAULT_MAX_PINGS_OUT;
        private SSLContext sslContext;
        private boolean tlsDebug;
        private TcpConnectionFactory factory;
        DisconnectedCallback disconnectedCb;
        ClosedCallback closedCb;
        ReconnectedCallback reconnectedCb;
        ExceptionHandler asyncErrorCb;

        /**
         * Constructs a {@link Builder} instance based on the supplied {@link Options} instance.
         * 
         * @param template the {@link Options} object to use as a template
         */
        public Builder(Options template) {
            this.url = template.url;
            this.username = template.username;
            this.password = template.password;
            this.token = template.token;
            if (template.servers != null) {
                this.servers = new ArrayList<URI>(template.servers);
            }
            this.noRandomize = template.noRandomize;
            this.connectionName = template.connectionName;
            this.verbose = template.pedantic;
            this.pedantic = template.pedantic;
            this.secure = template.secure;
            this.allowReconnect = template.allowReconnect;
            this.maxReconnect = template.maxReconnect;
            this.reconnectBufSize = template.reconnectBufSize;
            this.reconnectWait = template.reconnectWait;
            this.connectionTimeout = template.connectionTimeout;
            this.pingInterval = template.pingInterval;
            this.maxPingsOut = template.maxPingsOut;
            this.sslContext = template.sslContext;
            this.tlsDebug = template.tlsDebug;
            this.disconnectedCb = template.disconnectedCb;
            this.closedCb = template.closedCb;
            this.reconnectedCb = template.reconnectedCb;
            this.asyncErrorCb = template.asyncErrorCb;
            this.factory = template.factory;
        }

        public Builder() {}

        /**
         * Constructs a new {@code Builder} from a {@link Properties} object.
         *
         * @param props the {@link Properties} object
         */
        public Builder(Properties props) {
            if (props == null) {
                throw new IllegalArgumentException("Properties cannot be null");
            }

            // PROP_URL
            if (props.containsKey(PROP_URL)) {
                this.url = props.getProperty(PROP_URL, DEFAULT_URL);
            }
            // PROP_USERNAME
            if (props.containsKey(PROP_USERNAME)) {
                this.username = props.getProperty(PROP_USERNAME, null);
            }
            // PROP_PASSWORD
            if (props.containsKey(PROP_PASSWORD)) {
                this.password = props.getProperty(PROP_PASSWORD, null);
            }
            // PROP_SERVERS
            if (props.containsKey(PROP_SERVERS)) {
                String str = props.getProperty(PROP_SERVERS);
                if (str.isEmpty()) {
                    throw new IllegalArgumentException(PROP_SERVERS + " cannot be empty");
                } else {
                    String[] servers = str.trim().split(",\\s*");
                    this.servers = processServers(servers);
                }
            }
            // PROP_NORANDOMIZE
            if (props.containsKey(PROP_NORANDOMIZE)) {
                this.noRandomize = Boolean.parseBoolean(props.getProperty(PROP_NORANDOMIZE));
            }
            // PROP_CONNECTION_NAME
            if (props.containsKey(PROP_CONNECTION_NAME)) {
                this.connectionName = props.getProperty(PROP_CONNECTION_NAME, null);
            }
            // PROP_VERBOSE
            if (props.containsKey(PROP_VERBOSE)) {
                this.verbose = Boolean.parseBoolean(props.getProperty(PROP_VERBOSE));
            }
            // PROP_PEDANTIC
            if (props.containsKey(PROP_PEDANTIC)) {
                this.pedantic = Boolean.parseBoolean(props.getProperty(PROP_PEDANTIC));
            }
            // PROP_SECURE
            if (props.containsKey(PROP_SECURE)) {
                this.secure = Boolean.parseBoolean(props.getProperty(PROP_SECURE));
            }
            // PROP_TLS_DEBUG
            if (props.containsKey(PROP_TLS_DEBUG)) {
                this.tlsDebug = Boolean.parseBoolean(props.getProperty(PROP_TLS_DEBUG));
            }
            // PROP_RECONNECT_ALLOWED
            if (props.containsKey(PROP_RECONNECT_ALLOWED)) {
                this.allowReconnect = Boolean.parseBoolean(
                        props.getProperty(PROP_RECONNECT_ALLOWED, Boolean.toString(true)));
            }
            // PROP_MAX_RECONNECT
            if (props.containsKey(PROP_MAX_RECONNECT)) {
                this.maxReconnect = Integer.parseInt(props.getProperty(PROP_MAX_RECONNECT,
                        Integer.toString(DEFAULT_MAX_RECONNECT)));
            }
            // PROP_RECONNECT_WAIT
            if (props.containsKey(PROP_RECONNECT_WAIT)) {
                this.reconnectWait = Integer.parseInt(props.getProperty(PROP_RECONNECT_WAIT,
                        Integer.toString(DEFAULT_RECONNECT_WAIT)));
            }
            // PROP_RECONNECT_BUF_SIZE
            if (props.containsKey(PROP_RECONNECT_BUF_SIZE)) {
                this.reconnectBufSize = Integer.parseInt(props.getProperty(PROP_RECONNECT_BUF_SIZE,
                        Integer.toString(DEFAULT_RECONNECT_BUF_SIZE)));
            }
            // PROP_CONNECTION_TIMEOUT
            if (props.containsKey(PROP_CONNECTION_TIMEOUT)) {
                this.connectionTimeout = Integer.parseInt(
                        props.getProperty(PROP_CONNECTION_TIMEOUT, Integer.toString(DEFAULT_TIMEOUT)));
            }
            // PROP_PING_INTERVAL
            if (props.containsKey(PROP_PING_INTERVAL)) {
                this.pingInterval = Integer.parseInt(props.getProperty(PROP_PING_INTERVAL,
                        Integer.toString(DEFAULT_PING_INTERVAL)));
            }
            // PROP_MAX_PINGS
            if (props.containsKey(PROP_MAX_PINGS)) {
                this.maxPingsOut = Integer.parseInt(
                        props.getProperty(PROP_MAX_PINGS, Integer.toString(DEFAULT_MAX_PINGS_OUT)));
            }
            // PROP_EXCEPTION_HANDLER
            if (props.containsKey(PROP_EXCEPTION_HANDLER)) {
                Object instance = null;
                try {
                    String str = props.getProperty(PROP_EXCEPTION_HANDLER);
                    Class<?> clazz = Class.forName(str);
                    Constructor<?> constructor = clazz.getConstructor();
                    instance = constructor.newInstance();
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                } finally {
                /* NOOP */
                }
                this.asyncErrorCb = (ExceptionHandler) instance;
            }
            // PROP_CLOSED_CB
            if (props.containsKey(PROP_CLOSED_CB)) {
                Object instance = null;
                try {
                    String str = props.getProperty(PROP_CLOSED_CB);
                    Class<?> clazz = Class.forName(str);
                    Constructor<?> constructor = clazz.getConstructor();
                    instance = constructor.newInstance();
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }
                this.closedCb = (ClosedCallback) instance;
            }
            // PROP_DISCONNECTED_CB
            if (props.containsKey(PROP_DISCONNECTED_CB)) {
                Object instance = null;
                try {
                    String str = props.getProperty(PROP_DISCONNECTED_CB);
                    Class<?> clazz = Class.forName(str);
                    Constructor<?> constructor = clazz.getConstructor();
                    instance = constructor.newInstance();
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }
                this.disconnectedCb = (DisconnectedCallback) instance;
            }
            // PROP_RECONNECTED_CB
            if (props.containsKey(PROP_RECONNECTED_CB)) {
                Object instance = null;
                try {
                    String str = props.getProperty(PROP_RECONNECTED_CB);
                    Class<?> clazz = Class.forName(str);
                    Constructor<?> constructor = clazz.getConstructor();
                    instance = constructor.newInstance();
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }
                this.reconnectedCb = (ReconnectedCallback) instance;
            }
        }

        public Builder dontRandomize() {
            this.noRandomize = true;
            return this;
        }

        Builder factory(TcpConnectionFactory factory) {
            this.factory = factory;
            return this;
        }

        public Builder maxPingsOut(int maxPingsOut) {
            this.maxPingsOut = maxPingsOut;
            return this;
        }

        public Builder maxReconnect(int maxReconnect) {
            this.maxReconnect = maxReconnect;
            return this;
        }

        public Builder name(String connectionName) {
            this.connectionName = connectionName;
            return this;
        }

        public Builder pedantic() {
            this.pedantic = true;
            return this;
        }

        public Builder pingInterval(long interval, TimeUnit unit) {
            return pingInterval(unit.toMillis(interval));
        }

        public Builder pingInterval(long pingInterval) {
            this.pingInterval = pingInterval;
            return this;
        }

        public Builder noReconnect() {
            this.allowReconnect = false;
            return this;
        }

        public Builder reconnectBufSize(int reconnectBufSize) {
            this.reconnectBufSize = reconnectBufSize;
            return this;
        }

        public Builder reconnectWait(long millis) {
            this.reconnectWait = millis;
            return this;
        }

        public Builder reconnectWait(long duration, TimeUnit unit) {
            return reconnectWait(unit.toMillis(duration));
        }

        public Builder secure() {
            this.secure = true;
            return this;
        }

        public List<URI> processServers(String[] serverArray) {
            List<URI> list = null;
            if ((serverArray != null) && (serverArray.length != 0)) {
                list = new ArrayList<URI>(serverArray.length);
                for (String s : serverArray) {
                    if (s != null && !s.isEmpty()) {
                        try {
                            list.add(new URI(s.trim()));
                        } catch (URISyntaxException e) {
                            throw new IllegalArgumentException("Bad server URL: " + s);
                        }
                    }
                }
            }
            return list;
        }

//        public Builder servers(List<URI> servers) {
//            this.servers = servers;
//            return this;
//        }

        public Builder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            if (sslContext != null) {
                this.secure = true;
            }
            return this;
        }

        public Builder timeout(int millis) {
            this.connectionTimeout = millis;
            return this;
        }

        public Builder timeout(int timeout, TimeUnit unit) {
            return timeout((int)unit.toMillis(timeout));
        }

        public Builder tlsDebug() {
            this.tlsDebug = true;
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder userInfo(String user, String pass) {
            this.username = user;
            this.password = pass;
            return this;
        }

        public Builder verbose() {
            this.verbose = true;
            return this;
        }

        public Builder errorCb(ExceptionHandler cb) {
            this.asyncErrorCb = cb;
            return this;
        }

        public Builder closedCb(ClosedCallback cb) {
            this.closedCb = cb;
            return this;
        }

        public Builder disconnectedCb(DisconnectedCallback cb) {
            this.disconnectedCb = cb;
            return this;
        }

        public Builder reconnectedCb(ReconnectedCallback cb) {
            this.reconnectedCb = cb;
            return this;
        }

        /**
         * Creates a {@link Options} instance based on the current configuration.
         * 
         * @return the created {@link Options} instance
         */
        public Options build() {
            return new Options(this);
        }

    }
}
