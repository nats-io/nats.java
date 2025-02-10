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
import io.nats.client.support.NatsUri;
import io.nats.client.support.WebSocket;

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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.nats.client.support.NatsConstants.SECURE_WEBSOCKET_PROTOCOL;

/**
 * This class is not thread-safe.  Caller must ensure thread safety.
 */
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
    public void connect(String serverURI, NatsConnection conn, long timeoutNanos) throws IOException {
        try {
            connect(conn, new NatsUri(serverURI), timeoutNanos);
        }
        catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void connect(NatsConnection conn, NatsUri nuri, long timeoutNanos) throws IOException {
        connection = conn;
        Options options = connection.getOptions();
        long timeout = timeoutNanos / 1_000_000; // convert to millis
        host = nuri.getHost();
        port = nuri.getPort();

        try {
            if (options.getProxy() != null) {
                socket = new Socket(options.getProxy());
            }
            else {
                socket = new Socket();
            }
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(2 * 1024 * 1024);
            socket.setSendBufferSize(2 * 1024 * 1024);
            socket.connect(new InetSocketAddress(resolveHostToIps(nuri).get(0).getHost(), port), (int) timeout);
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
            try { socket.close(); } catch (Exception ignore) {}
            socket = null;
            if (e instanceof IOException) {
                throw e;
            }
            throw new IOException(e);
        }
    }

    private List<NatsUri> resolveHostToIps(NatsUri nuri) {
        if (nuri.hostIsIpAddress()) {
            return Arrays.asList(nuri);
        }
        try {
            InetAddress[] addresses = InetAddress.getAllByName(nuri.getHost());
            return rehostToNUri(Arrays.asList(addresses), nuri);
        } catch (UnknownHostException e) {
            System.out.println(String.format("[WARN] Ignoring Address %s as Error in resolving host: %s", nuri.getHost(), e.getMessage()));
        }
        return Arrays.asList(nuri);
    }


    private List<NatsUri> rehostToNUri(List<InetAddress> inetAddresses, NatsUri nuri) {
        List<NatsUri> natsUris = new ArrayList<>();
        for (InetAddress addr : inetAddresses) {
            try {
                if (addr instanceof Inet6Address) {
                    continue;
                }
                natsUris.add(nuri.reHost(addr.getHostAddress()));
            } catch (URISyntaxException e) {
                System.out.println(String.format("[WARN] Ignoring Address %s as Error in while rehostToNri: %s", nuri.getHost(), e.getMessage()));
            }
        }
        if (natsUris.size() < 1) {
            natsUris.add(nuri);
        }
        Collections.shuffle(natsUris, ThreadLocalRandom.current());
        return natsUris;
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
        
        sslSocket.addHandshakeCompletedListener((evt) -> {
            waitForHandshake.complete(null);
        });

        sslSocket.startHandshake();

        try {
            waitForHandshake.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Exception ex) {
            connection.handleCommunicationIssue(ex);
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

    @Override
    public void forceClose() throws IOException {
        try {
            // If we are being asked to force close, there is no need to linger.
            socket.setSoLinger(true, 0);
        }
        catch (SocketException e) {
            // don't want to fail if I couldn't set linger
        }
        close();
    }

    public void flush() throws IOException {
        out.flush();
    }

    protected static boolean isWebsocketScheme(String scheme) {
        return "ws".equalsIgnoreCase(scheme) ||
            "wss".equalsIgnoreCase(scheme);
    }
}
