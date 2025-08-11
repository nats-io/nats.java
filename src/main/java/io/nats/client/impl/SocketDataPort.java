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
import io.nats.client.support.NatsInetAddress;
import io.nats.client.support.NatsUri;
import io.nats.client.support.WebSocket;
import org.jspecify.annotations.NonNull;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static io.nats.client.support.NatsConstants.SECURE_WEBSOCKET_PROTOCOL;

/**
 * This class is not thread-safe.  Caller must ensure thread safety.
 */
@SuppressWarnings("ClassEscapesDefinedScope") // NatsConnection
public class SocketDataPort implements DataPort {

    protected NatsConnection connection;

    protected String host;
    protected int port;
    protected Socket socket;
    protected boolean isSecure = false;
    protected int soLinger;

    protected InputStream in;
    protected OutputStream out;

    @Override
    public void afterConstruct(Options options) {
        soLinger = options.getSocketSoLinger();
    }

    @Override
    public void connect(@NonNull String serverURI, @NonNull NatsConnection conn, long timeoutNanos) throws IOException {
        try {
            connect(conn, new NatsUri(serverURI), timeoutNanos);
        }
        catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void connect(@NonNull NatsConnection conn, @NonNull NatsUri nuri, long timeoutNanos) throws IOException {
        connection = conn;
        Options options = connection.getOptions();
        long timeout = timeoutNanos / 1_000_000; // convert to millis
        host = nuri.getHost();
        port = nuri.getPort();

        try {
            if (options.isEnableFastFallback()) {
                socket = connectToFastestIp(options, host, port, (int) timeout);
            } else {
                socket = createSocket(options);
                socket.connect(new InetSocketAddress(host, port), (int) timeout);
            }

            if (soLinger > -1) {
                socket.setSoLinger(true, soLinger);
            }
            if (options.getSocketReadTimeoutMillis() > 0) {
                socket.setSoTimeout(options.getSocketReadTimeoutMillis());
            }

            if (isWebsocketScheme(nuri.getScheme())) {
                if (SECURE_WEBSOCKET_PROTOCOL.equalsIgnoreCase(nuri.getScheme())) {
                    upgradeToSecure();
                }
                try {
                    socket = new WebSocket(socket, host, options.getHttpRequestInterceptors());
                } catch (Exception ex) {
                    socket.close();
                    throw ex;
                }
            }
            in = socket.getInputStream();
            out = socket.getOutputStream();
        }
        catch (Exception e) {
            if (socket != null) {
                try { socket.close(); } catch (Exception ignore) {}
            }
            socket = null;
            if (e instanceof IOException) {
                throw e;
            }
            throw new IOException(e);
        }
    }

    /**
     * Upgrade the port to SSL. If it is already secured, this is a no-op.
     * If the data port type doesn't support SSL it should throw an exception.
     */
    public void upgradeToSecure() throws IOException {
        Options options = connection.getOptions();
        SSLContext context = options.getSslContext();

        SSLSocketFactory factory = context.getSocketFactory();
        Duration timeout = options.getConnectionTimeout();

        SSLSocket sslSocket = (SSLSocket) factory.createSocket(socket, host, port, true);
        sslSocket.setUseClientMode(true);

        final CompletableFuture<Void> waitForHandshake = new CompletableFuture<>();
        final HandshakeCompletedListener hcl = (evt) -> waitForHandshake.complete(null);

        sslSocket.addHandshakeCompletedListener(hcl);
        sslSocket.startHandshake();

        try {
            waitForHandshake.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Exception ex) {
            connection.handleCommunicationIssue(ex);
            return;
        }
        finally {
            sslSocket.removeHandshakeCompletedListener(hcl);
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
        if (!isSecure && socket != null) {
            socket.shutdownInput();
        }
    }

    public void close() throws IOException {
        if (socket != null) {
            socket.close();
        }
    }

    @Override
    public void forceClose() throws IOException {
        // socket can technically be null, like between states
        // practically it never will be, but guard it anyway
        if (socket != null) {
            try {
                // If we are being asked to force close, there is no need to linger.
                socket.setSoLinger(true, 0);
            }
            catch (SocketException e) {
                // don't want to fail if I couldn't set linger
            }
            close();
        }
    }

    public void flush() throws IOException {
        out.flush();
    }

    protected static boolean isWebsocketScheme(String scheme) {
        return "ws".equalsIgnoreCase(scheme) ||
            "wss".equalsIgnoreCase(scheme);
    }

    /**
     * Implements the "Happy Eyeballs" algorithm as described in RFC 6555,
     * which attempts to connect to multiple IP addresses in parallel to reduce
     * connection setup delays.
     */
    private Socket connectToFastestIp(Options options, String hostname, int port,
                                      int timeoutMillis) throws IOException {
        // Get all IP addresses for the hostname
        List<InetAddress> ips = Arrays.asList(NatsInetAddress.getAllByName(hostname));

        ExecutorService executor = options.getExecutor();
        long CONNECT_DELAY_MILLIS = 250;
        // Create connection tasks for each address
        // with delays for each address (0ms, 250ms, 500ms, ...)
        List<Callable<Socket>> connectionTasks = new ArrayList<>();

        for (int i = 0; i < ips.size(); i++) {
            final InetAddress ip = ips.get(i);
            final int delayMillis = i * (int) CONNECT_DELAY_MILLIS;

            connectionTasks.add(() -> {
                if (delayMillis > 0) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                Socket socket = createSocket(options);
                socket.connect(new InetSocketAddress(ip, port), timeoutMillis);
                return socket;
            });
        }

        try {
            // Use invokeAny to return the first successful connection and cancel other tasks
            return executor.invokeAny(connectionTasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException ignored) {
        }
        // Could not connect to any IP address
        throw new IOException("No responsive IP found for " + hostname);
    }

    private Socket createSocket(Options options) throws SocketException {
        Socket socket;
        if (options.getProxy() != null) {
            socket = new Socket(options.getProxy());
        } else {
            socket = new Socket();
        }
        socket.setTcpNoDelay(true);
        socket.setReceiveBufferSize(2 * 1024 * 1024);
        socket.setSendBufferSize(2 * 1024 * 1024);
        return socket;
    }
}
