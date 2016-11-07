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

public class Options {
    protected String url;
    String username;
    String password;
    String token;
    List<URI> servers;
    boolean noRandomize;
    String connectionName;
    boolean verbose;
    boolean pedantic;
    boolean secure;
    boolean allowReconnect;
    int maxReconnect;
    int reconnectBufSize;
    long reconnectWait;
    int connectionTimeout;
    long pingInterval;
    int maxPingsOut;
    TcpConnectionFactory factory;
    SSLContext sslContext;
    boolean tlsDebug;
    public DisconnectedCallback disconnectedCb;
    public ClosedCallback closedCb;
    public ReconnectedCallback reconnectedCb;
    public ExceptionHandler asyncErrorCb;

    // For ConnectionFactory
    String host;
    int port;

    // private List<X509Certificate> certificates =
    // new ArrayList<X509Certificate>();

    private Options(Builder builder) {
        this.factory = builder.factory;
        this.url = builder.url;
        this.host = builder.host;
        this.port = builder.port;
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

    /**
     * Creates a connection using this {@code Options} object.
     *
     * @return the {@code Connection}
     * @throws IOException if something goes wrong
     * @throws TimeoutException if the connection attempt times out
     */
    public Connection connect() throws IOException, TimeoutException {
        ConnectionImpl conn = new ConnectionImpl(this);
        conn.connect();
        return conn;
    }

    /**
     * Creates a connection using the supplied list of URLs and this {@code Options} object.
     * 
     * @param urls a comma-separated list of NATS server URLs
     * @return the {@code Connection}
     * @throws IOException if something goes wrong
     * @throws TimeoutException if the connection attempt times out
     */
    public Connection connect(String urls) throws IOException, TimeoutException {
        this.servers = processUrlString(urls);
        ConnectionImpl conn = new ConnectionImpl(this);
        conn.connect();
        return conn;
    }

    List<URI> processUrlString(String urls) {
        String[] list = urls.split(",");
        if (list.length == 1) {
            this.url = list[0];
        } else {
            for (String item : list) {
                servers.add(URI.create(item));
            }
        }
        return servers;
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

    public static final class Builder {
        private String url;
        private String username;
        private String password;
        private String token;
        private List<URI> servers;
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

        /*
         * For ConnectionFactory
         */
        private String host;
        private int port;

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
         * Constructs a new connection factory from a {@link Properties} object.
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
            // PROP_HOST
            if (props.containsKey(PROP_HOST)) {
                this.host = props.getProperty(PROP_HOST, DEFAULT_HOST);
            }
            // PROP_PORT
            if (props.containsKey(PROP_PORT)) {
                this.port =
                        Integer.parseInt(props.getProperty(PROP_PORT, Integer.toString(DEFAULT_PORT)));
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
                    this.servers(servers);
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

        public Builder url(String url) {
            if (url != null && !url.isEmpty()) {
                this.url = url;
            }
            return this;
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

        public Builder servers(String[] serverArray) {
            if ((serverArray != null) && (serverArray.length != 0)) {
                if (this.servers == null) {
                    this.servers = new ArrayList<URI>();
                }
                for (String s : serverArray) {
                    if (s != null && !s.isEmpty()) {
                        try {
                            this.servers.add(new URI(s.trim()));
                        } catch (URISyntaxException e) {
                            throw new IllegalArgumentException("Bad server URL: " + s);
                        }
                    } else {
                        continue;
                    }

                }
            }
            return this;
        }

        public Builder servers(List<URI> servers) {
            this.servers = servers;
            return this;
        }

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

        // public Builder url(URI url) {
        // this.url = url;
        // if (url != null) {
        // if (url.getHost() != null) {
        // this.host(url.getHost());
        // }
        // this.port(url.getPort());
        //
        // String userInfo = url.getRawUserInfo();
        // if (userInfo != null) {
        // String[] userPass = userInfo.split(":");
        // if (userPass.length > 2) {
        // throw new IllegalArgumentException(
        // "Bad user info in NATS " + "URI: " + userInfo);
        // }
        // userInfo(userPass[0], userPass[1]);
        // }
        // }
        // return this;
        // }

        // public Builder url(String url) {
        // if (url == null) {
        // return this;
        // } else {
        // if (url.isEmpty()) {
        // return this;
        // } else {
        // try {
        // this.url = new URI(url);
        // } catch (URISyntaxException e) {
        // throw new IllegalArgumentException("Bad server URL: " + url);
        // }
        // }
        // }
        // return this;
        // }

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
