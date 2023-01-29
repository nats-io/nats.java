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

package io.nats.client.impl;

import io.nats.client.Options;
import io.nats.client.support.WebSocket;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.nats.client.support.NatsConstants.SECURE_WEBSOCKET_PROTOCOL;

/**
 * This class is not theadsafe.  Caller must ensure thread safety.
 */
public class SocketDataPort implements DataPort {

    private NatsConnection connection;
    private String host;
    private int port;
    private Socket socket;
    private boolean isSecure = false;

    private InputStream in;
    private OutputStream out;

    public void connect(String serverURI, NatsConnection conn, long timeoutNanos) throws IOException {

        try {
            this.connection = conn;

            Options options = this.connection.getOptions();
            long timeout = timeoutNanos / 1_000_000; // convert to millis
            URI uri = options.createURIForServer(serverURI);
            this.host = uri.getHost();
            this.port = uri.getPort();

            if (options.getProxy() != null) {
                this.socket = new Socket(options.getProxy());
            } else {
                this.socket = new Socket();
            }
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(2 * 1024 * 1024);
            socket.setSendBufferSize(2 * 1024 * 1024);
            socket.connect(new InetSocketAddress(host, port), (int) timeout);

            if (isWebsocketScheme(uri.getScheme())) {
                if (SECURE_WEBSOCKET_PROTOCOL.equalsIgnoreCase(uri.getScheme())) {
                    upgradeToSecure();
                }
                try {
                    socket = new WebSocket(socket, this.host, options.getHttpRequestInterceptors());
                } catch (Exception ex) {
                    socket.close();
                    throw ex;
                }
            }
            in = socket.getInputStream();
            out = socket.getOutputStream();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Upgrade the port to SSL. If it is already secured, this is a no-op.
     * If the data port type doesn't support SSL it should throw an exception.
     */
    public void upgradeToSecure() throws IOException {
        Options options = this.connection.getOptions();
        SSLContext context = options.getSslContext();
        
        SSLSocketFactory factory = context.getSocketFactory();
        Duration timeout = options.getConnectionTimeout();

        SSLSocket sslSocket = (SSLSocket) factory.createSocket(socket, this.host, this.port, true);
        sslSocket.setUseClientMode(true);

        final CompletableFuture<Void> waitForHandshake = new CompletableFuture<>();
        
        sslSocket.addHandshakeCompletedListener((evt) -> {
            waitForHandshake.complete(null);
        });

        sslSocket.startHandshake();

        try {
            waitForHandshake.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Exception ex) {
            this.connection.handleCommunicationIssue(ex);
            return;
        }

        socket = sslSocket;
        in = sslSocket.getInputStream();
        out = sslSocket.getOutputStream();
        isSecure = true;
    }

    public int read(byte[] dst, int off, int len) throws IOException {
        return in.read(dst, off, len);
    }

    public void write(byte[] src, int toWrite) throws IOException {
        out.write(src, 0, toWrite);
    }

    public void shutdownInput() throws IOException {
        // cannot call shutdownInput on sslSocket
        if (!isSecure) {
            socket.shutdownInput();
        }
    }

    public void close() throws IOException {
        socket.close();
    }

    public void flush() throws IOException {
        out.flush();
    }

    private static boolean isWebsocketScheme(String scheme) {
        return "ws".equalsIgnoreCase(scheme) ||
            "wss".equalsIgnoreCase(scheme);
    }
}
