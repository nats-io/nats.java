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

import static java.lang.System.in;

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

    // TODO: Test various scenarios for efficiency. Is a
    // BufferedReader directly over a network stream really
    // more efficient for NATS?
    //
    private final ReentrantLock mu = new ReentrantLock();
    private SocketFactory factory = SocketFactory.getDefault();
    private SSLContext sslContext;
    private Socket client = null;
    private OutputStream writeStream = null;
    private InputStream readStream = null;
    private BufferedReader bisr = null;
    private BufferedInputStream bis = null;
    private BufferedOutputStream bos = null;

    private int timeout = 0;

    TcpConnection() {
    }

    @Override
    public void open(String url, int timeout) throws IOException {
        URI uri = URI.create(url);
        String host = uri.getHost();
        int port = uri.getPort();
        mu.lock();
        try {

            client = factory.createSocket();
            client.setTcpNoDelay(true);
            client.setReceiveBufferSize(2 * 1024 * 1024);
            client.setSendBufferSize(2 * 1024 * 1024);
            client.connect(new InetSocketAddress(host, port), timeout);

            writeStream = client.getOutputStream();
            readStream = client.getInputStream();

        } finally {
            mu.unlock();
        }
    }

    void setConnectTimeout(int value) {
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

    @Override
    public boolean isConnected() {
        return client != null && client.isConnected();
    }

    @Override
    public boolean isClosed() {
        return client.isClosed();
    }

    /**
     * Set the socket factory used to make connections with. Can be used to enable SSL connections
     * by passing in a javax.net.ssl.SSLSocketFactory instance.
     *
     * @see #makeTls
     */
    void setSocketFactory(SocketFactory factory) {
        this.factory = factory;
    }

    protected SSLSocketFactory getSslSocketFactory() {
        if (factory instanceof SSLSocketFactory) {
            return (SSLSocketFactory) factory;
        } else {
            return null;
        }
    }

    void makeTls(SSLContext context) throws IOException {
        this.sslContext = context;
        setSocketFactory(sslContext.getSocketFactory());
        makeTls();
    }

    void makeTls() throws IOException {
        SSLSocketFactory sslSf = getSslSocketFactory();
        SSLSocket sslSocket = (SSLSocket) sslSf.createSocket(client,
                client.getInetAddress().getHostAddress(), client.getPort(), true);

        sslSocket.startHandshake();
        this.bisr = null;
        this.readStream = sslSocket.getInputStream();
        bis = null;
        this.writeStream = sslSocket.getOutputStream();
        bos = null;
    }

    void setSocket(Socket sock) {
        mu.lock();
        try {
            client = sock;
        } finally {
            mu.unlock();
        }
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

    protected void setReadStream(InputStream in) {
        readStream = in;
    }

    protected InputStream getReadStream() {
        return readStream;
    }

    protected void setWriteStream(OutputStream out) {
        writeStream = out;
    }

    protected OutputStream getWriteStream() {
        return writeStream;
    }

    protected int getTimeout() {
        return timeout;
    }
}
