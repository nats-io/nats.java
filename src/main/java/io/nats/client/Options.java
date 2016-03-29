/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;

class Options {
    private URI url;
    private String host;
    private int port;
    private String username;
    private String password;
    private List<URI> servers;
    private boolean noRandomize;
    private String connectionName;
    private boolean verbose;
    private boolean pedantic;
    private boolean secure;
    private boolean reconnectAllowed;
    private int maxReconnect;
    private int reconnectBufSize;
    private long reconnectWait;
    private int connectionTimeout;
    private long pingInterval;
    private int maxPingsOut;
    private ExceptionHandler exceptionHandler;
    private SSLContext sslContext;
    private boolean tlsDebug;
    private int maxPendingMsgs;
    private long maxPendingBytes;
    protected DisconnectedCallback disconnectedCB;
    protected ClosedCallback closedCB;
    protected ReconnectedCallback reconnectedCB;
    protected ExceptionHandler asyncErrorCB;

    // private List<X509Certificate> certificates =
    // new ArrayList<X509Certificate>();

    public int getMaxPendingMsgs() {
        return maxPendingMsgs;
    }

    public void setMaxPendingMsgs(int max) {
        this.maxPendingMsgs = max;
    }

    public long getMaxPendingBytes() {
        return maxPendingBytes;
    }

    public void setMaxPendingBytes(long max) {
        this.maxPendingBytes = max;
    }

    public URI getUrl() {
        return url;
    }

    public void setUrl(URI url) {
        this.url = url;
        if (url != null) {
            if (url.getHost() != null) {
                this.setHost(url.getHost());
            }
            this.setPort(url.getPort());

            String userInfo = url.getRawUserInfo();
            if (userInfo != null) {
                String userPass[] = userInfo.split(":");
                if (userPass.length > 2) {
                    throw new IllegalArgumentException(
                            "Bad user info in NATS " + "URI: " + userInfo);
                }

                setUsername(userPass[0]);
                if (userPass.length == 2) {
                    setPassword(userPass[1]);
                }
            }
        }
    }

    public void setUrl(String url) {
        if (url == null) {
            return;
        } else {
            if (url.isEmpty()) {
                return;
            } else {
                try {
                    this.url = new URI(url);
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException("Bad server URL: " + url);
                }
            }
        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<URI> getServers() {
        return servers;
    }

    public void setServers(String[] serverArray) {
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
    }

    public void setServers(List<URI> servers) {
        this.servers = servers;
    }

    public boolean isNoRandomize() {
        return noRandomize;
    }

    public void setNoRandomize(boolean randomizeDisabled) {
        this.noRandomize = randomizeDisabled;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean isPedantic() {
        return pedantic;
    }

    public void setPedantic(boolean pedantic) {
        this.pedantic = pedantic;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public boolean isTlsDebug() {
        return tlsDebug;
    }

    public void setTlsDebug(boolean debug) {
        this.tlsDebug = debug;
    }

    public boolean isReconnectAllowed() {
        return reconnectAllowed;
    }

    public void setReconnectAllowed(boolean reconnectAllowed) {
        this.reconnectAllowed = reconnectAllowed;
    }

    public int getMaxReconnect() {
        return maxReconnect;
    }

    public void setMaxReconnect(int maxReconnect) {
        this.maxReconnect = maxReconnect;
    }

    public int getReconnectBufSize() {
        return reconnectBufSize;
    }

    public void setReconnectBufSize(int reconnectBufSize) {
        this.reconnectBufSize = reconnectBufSize;
    }

    public long getReconnectWait() {
        return reconnectWait;
    }

    public void setReconnectWait(long reconnectWait) {
        this.reconnectWait = reconnectWait;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public long getPingInterval() {
        return pingInterval;
    }

    public void setPingInterval(long pingInterval) {
        this.pingInterval = pingInterval;
    }

    public int getMaxPingsOut() {
        return maxPingsOut;
    }

    public void setMaxPingsOut(int maxPingsOut) {
        this.maxPingsOut = maxPingsOut;
    }

    public ExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    public ClosedCallback getClosedCallback() {
        return closedCB;
    }

    public void setClosedCallback(ClosedCallback cb) {
        this.closedCB = cb;
    }

    public ReconnectedCallback getReconnectedCallback() {
        return reconnectedCB;
    }

    public void setReconnectedCallback(ReconnectedCallback cb) {
        this.reconnectedCB = cb;
    }

    public DisconnectedCallback getDisconnectedCallback() {
        return disconnectedCB;
    }

    public void setDisconnectedCallback(DisconnectedCallback cb) {
        this.disconnectedCB = cb;
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

    public SSLContext getSSLContext() {
        return sslContext;
    }

    public void setSSLContext(SSLContext sslContext) {
        this.sslContext = sslContext;
        if (sslContext != null) {
            setSecure(true);
        }
    }
}
