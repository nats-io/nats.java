/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.security.cert.Certificate;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.SocketFactory;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/*
 * Convenience class representing the TCP connection to prevent managing two variables throughout
 * the NATS client code.
 */
class TcpConnection implements TransportConnection, AutoCloseable {
    final Logger logger = LoggerFactory.getLogger(TcpConnection.class);

    /// TODO: Test various scenarios for efficiency. Is a
    /// BufferedReader directly over a network stream really
    /// more efficient for NATS?
    ///
    ReentrantLock mu = new ReentrantLock();
    protected SocketFactory factory = SocketFactory.getDefault();
    protected SSLContext sslContext;
    Socket client = null;
    protected OutputStream writeStream = null;
    protected InputStream readStream = null;
    protected BufferedReader bisr = null;
    protected BufferedInputStream bis = null;
    protected BufferedOutputStream bos = null;

    protected InetSocketAddress addr = null;
    protected int timeout = 0;
    boolean tlsDebug = false;

    TcpConnection() {
    }

    @Override
    public void open(String url, int timeout) throws IOException, TimeoutException {
        logger.trace("TcpConnection.open({},{})", url, timeout);
        URI uri = URI.create(url);
        String host = uri.getHost();
        int port = uri.getPort();
        mu.lock();
        try {

            this.addr = new InetSocketAddress(host, port);
            client = factory.createSocket();
            client.setTcpNoDelay(true);
            client.setReceiveBufferSize(2 * 1024 * 1024);
            client.setSendBufferSize(2 * 1024 * 1024);
            client.connect(addr, timeout);

            logger.debug("socket tcp_nodelay: {}", client.getTcpNoDelay());
            logger.debug("socket recv buf size: {}", client.getReceiveBufferSize());
            logger.debug("socket send buf size: {}", client.getSendBufferSize());

            writeStream = client.getOutputStream();
            readStream = client.getInputStream();

        } catch (IOException e) {
            throw e;
        } finally {
            mu.unlock();
        }
    }

    protected void setConnectTimeout(int value) {
        this.timeout = value;
    }

    // setSendTimeout?

    boolean isSetup() {
        return (client != null);
    }

    void teardown() {
        mu.lock();
        try {
            if (client != null) {
                client.close();
            }
            client = null;
            writeStream = null;
            readStream = null;
            bisr = null;
            bis = null;
            bos = null;
        } catch (IOException e) {
            // ignore
        } finally {
            mu.unlock();
        }
    }

    @Override
    public BufferedReader getBufferedReader() {
        return new BufferedReader(new InputStreamReader(bis));
    }

    @Override
    public InputStream getInputStream(int size) {
        if (bis == null) {
            if (size > 0) {
                bis = new BufferedInputStream(readStream, size);
            } else {
                bis = new BufferedInputStream(readStream);
            }
            bis = new BufferedInputStream(readStream, size);
        }
        return bis;
    }

    @Override
    public OutputStream getOutputStream(int size) {
        if (bos == null) {
            bos = new BufferedOutputStream(writeStream, size);
        }
        return bos;
    }

    OutputStream getOutputStream() {
        // if (bos == null) {
        // bos = new BufferedOutputStream(writeStream, size);
        // }
        return writeStream;
    }

    @Override
    public boolean isConnected() {
        if (client == null) {
            return false;
        }
        return client.isConnected();
    }

    @Override
    public boolean isClosed() {
        return client.isClosed();
    }

    /**
     * Set the socket factory used to make connections with. Can be used to enable SSL connections
     * by passing in a javax.net.ssl.SSLSocketFactory instance.
     *
     * @see #makeTLS
     */
    protected void setSocketFactory(SocketFactory factory) {
        this.factory = factory;
    }

    protected SSLSocketFactory getSslSocketFactory() {
        if (factory instanceof SSLSocketFactory) {
            return (SSLSocketFactory) factory;
        } else {
            return null;
        }
    }

    protected void makeTLS(SSLContext context) throws IOException {
        this.sslContext = context;
        setSocketFactory(sslContext.getSocketFactory());
        makeTLS();
    }

    protected void makeTLS() throws IOException {
        SSLSocketFactory sslSf = getSslSocketFactory();
        SSLSocket sslSocket = (SSLSocket) sslSf.createSocket(client,
                client.getInetAddress().getHostAddress(), client.getPort(), true);

        if (isTlsDebug()) {
            sslSocket.addHandshakeCompletedListener(new HandshakeListener());
        }

        logger.trace("Starting TLS handshake");
        sslSocket.startHandshake();
        logger.trace("TLS handshake complete");
        this.bisr = null;
        this.readStream = sslSocket.getInputStream();
        bis = null;
        this.writeStream = sslSocket.getOutputStream();
        bos = null;
    }

    protected void setSocket(Socket sock) {
        mu.lock();
        try {
            client = sock;
        } finally {
            mu.unlock();
        }
    }

    class HandshakeListener implements HandshakeCompletedListener {
        public void handshakeCompleted(javax.net.ssl.HandshakeCompletedEvent event) {
            SSLSession session = event.getSession();
            logger.trace("Handshake Completed with peer {}", session.getPeerHost());
            logger.trace("   cipher: {}", session.getCipherSuite());
            Certificate[] certs = null;
            try {
                certs = session.getPeerCertificates();
            } catch (SSLPeerUnverifiedException puv) {
                certs = null;
            }
            if (certs != null) {
                logger.trace("   peer certificates:");
                for (int z = 0; z < certs.length; z++) {
                    logger.trace("      certs[{}]: {}", z, certs[z]);
                }
            } else {
                logger.trace("No peer certificates presented");
            }
        }
    }

    boolean isTlsDebug() {
        return tlsDebug;
    }

    void setTlsDebug(boolean tlsDebug) {
        this.tlsDebug = tlsDebug;
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                /* NOOP */
            }
        }
        teardown();
    }

    protected SSLContext getSslContext() {
        return sslContext;
    }

    protected void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }
}
