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

import io.nats.client.HttpRequest;
import io.nats.client.Options;
import io.nats.client.support.NatsConstants;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class is not theadsafe.  Caller must ensure thread safety.
 */
public class SocketDataPort implements DataPort {
    private static final int MAX_LINE_LEN = 8192;
    private static final int MAX_HTTP_HEADERS = 100;
    private static final String WEBSOCKET_RESPONSE_LINE =
        "HTTP/1.1 101 Switching Protocols";

    private NatsConnection connection;
    private String host;
    private int port;
    private Socket socket;
    private SSLSocket sslSocket;

    private InputStream in;
    private OutputStream out;

    @Override
    public void connect(String serverURI, NatsConnection conn, long timeoutNanos) throws IOException {

        try {
            this.connection = conn;

            Options options = this.connection.getOptions();
            long timeout = timeoutNanos / 1_000_000; // convert to millis
            URI uri = options.createURIForServer(serverURI);
            this.host = uri.getHost();
            this.port = uri.getPort();

            this.socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(2 * 1024 * 1024);
            socket.setSendBufferSize(2 * 1024 * 1024);
            socket.connect(new InetSocketAddress(host, port), (int) timeout);

            in = socket.getInputStream();
            out = socket.getOutputStream();
            if (isWebsocketScheme(uri.getScheme())) {
                boolean isTLS = NatsConstants.SECURE_WEBSOCKET_PROTOCOL.equalsIgnoreCase(uri.getScheme());
                if (isTLS) {
                    upgradeToSecure();
                }
                websocketHandshake(isTLS, options.getHttpRequestInterceptors());
                in = new WebsocketInputStream(in);
                out = new WebsocketOutputStream(out, true);
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Upgrade the port to SSL. If it is already secured, this is a no-op.
     * If the data port type doesn't support SSL it should throw an exception.
     */
    @Override
    public void upgradeToSecure() throws IOException {
        try {
            upgradeToSecureImpl();
        } catch (Exception ex) {
            this.connection.handleCommunicationIssue(ex);
            return;
        }
        in = sslSocket.getInputStream();
        out = sslSocket.getOutputStream();
    }

    private void upgradeToSecureImpl() throws Exception {
        if (null != sslSocket) {
            return;
        }
        Options options = this.connection.getOptions();
        SSLContext context = options.getSslContext();
        
        SSLSocketFactory factory = context.getSocketFactory();
        Duration timeout = options.getConnectionTimeout();

        this.sslSocket = (SSLSocket) factory.createSocket(socket, this.host, this.port, true);
        this.sslSocket.setUseClientMode(true);

        final CompletableFuture<Void> waitForHandshake = new CompletableFuture<>();
        
        this.sslSocket.addHandshakeCompletedListener((evt) -> {
            waitForHandshake.complete(null);
        });

        this.sslSocket.startHandshake();

        waitForHandshake.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int read(byte[] dst, int off, int len) throws IOException {
        return in.read(dst, off, len);
    }

    /**
     * NOTE: the buffer will be modified if communicating over websockets and
     * the toWrite is greater than 1432.
     */
    @Override
    public void write(byte[] src, int toWrite) throws IOException {
        out.write(src, 0, toWrite);
    }

    @Override
    public void shutdownInput() throws IOException {
        // cannot call shutdownInput on sslSocket
        if (sslSocket == null) {
            socket.shutdownInput();
        }
    }

    @Override
    public void close() throws IOException {
        if (sslSocket != null) {
            sslSocket.close(); // auto closes the underlying socket
        } else {
            socket.close();
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    // TODO: Set a connection timeout for the handshake.
    private void websocketHandshake(boolean isWSS, List<Consumer<HttpRequest>> interceptors) throws IOException {

        HttpRequest request = new HttpRequest();

        // The value of this header field MUST be a
        // nonce consisting of a randomly selected 16-byte value that has
        // been base64-encoded
        byte[] keyBytes = new byte[16];
        new SecureRandom().nextBytes(keyBytes);
        String key = Base64.getEncoder().encodeToString(keyBytes);

        request.getHeaders()
            .add("Host", this.host)
            .add("Upgrade", "websocket")
            .add("Connection", "Upgrade")
            .add("Sec-WebSocket-Key", key)
            .add("Origin", (isWSS ? "https" : "http") + "://" + this.host + ":" + this.port)
            .add("Sec-WebSocket-Protocol", "nats")
            .add("Sec-WebSocket-Version", "13");
            // TODO: Support Sec-WebSocket-Extensions: permessage-deflate
            // TODO: Support Nats-No-Masking: TRUE

        for (Consumer<HttpRequest> interceptor : interceptors) {
            interceptor.accept(request);
        }
        out.write(request.toString().getBytes(UTF_8));

        // rfc6455 4.1 "The client MUST validate the server's response as follows:"
        byte[] buffer = new byte[MAX_LINE_LEN];
        String responseLine = readLine(buffer, in);
        if (null == responseLine) {
            throw new IllegalStateException("Expected HTTP response line not to exceed " + MAX_LINE_LEN);
        }
        // 1. expect 101:
        if (!responseLine.toLowerCase().startsWith(WEBSOCKET_RESPONSE_LINE.toLowerCase())) {
            throw new IllegalStateException("Expected " + WEBSOCKET_RESPONSE_LINE + ", but got " + responseLine);
        }
        Map<String, String> headers = new HashMap<>();
        while (true) {
            String line = readLine(buffer, in);
            if (null == line) {
                throw new IllegalStateException("Expected HTTP header to not exceed " + MAX_LINE_LEN);
            }
            byte[] bytes = line.getBytes(UTF_8);
            if ("".equals(line)) {
                break;
            }
            int colon = line.indexOf(':');
            if (colon >= 0) {
                if (headers.size() >= MAX_HTTP_HEADERS) {
                    throw new IllegalStateException("Exceeded max HTTP headers=" + MAX_HTTP_HEADERS);
                }
                headers.put(
                    line.substring(0, colon).trim().toLowerCase(),
                    line.substring(colon + 1).trim());
            } else {
                throw new IllegalStateException("Expected HTTP header to contain ':', but got " + line);
            }
        }
        // 2. Expect `Upgrade: websocket`
        if (!"websocket".equalsIgnoreCase(headers.get("upgrade"))) {
            throw new IllegalStateException(
                "Expected HTTP `Upgrade: websocket` header");
        }
        // 3. Expect `Connection: Upgrade`
        if (!"upgrade".equalsIgnoreCase(headers.get("connection"))) {
            throw new IllegalStateException(
                "Expected HTTP `Connection: Upgrade` header");
        }
        // 4. Sec-WebSocket-Accept: base64(sha1(key + "258EAF..."))
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(ex);
        }
        sha1.update(key.getBytes(UTF_8));
        sha1.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11".getBytes(UTF_8));
        String acceptKey = Base64.getEncoder().encodeToString(
            sha1.digest());
        String gotAcceptKey = headers.get("sec-websocket-accept");
        if (!acceptKey.equals(gotAcceptKey)) {
            throw new IllegalStateException(
                "Expected HTTP `Sec-WebSocket-Accept: " + acceptKey + ", but got " + gotAcceptKey);
        }
        // 5 & 6 are not valid, since nats-server doesn't
        // implement extensions or protocols.
    }

    private static boolean isWebsocketScheme(String scheme) {
        return "ws".equalsIgnoreCase(scheme) ||
            "wss".equalsIgnoreCase(scheme);
    }

    private static String readLine(byte[] buffer, InputStream in) throws IOException {
        int offset = 0;
        int lastCh = -1;
        while (true) {
            int ch = in.read();
            switch (ch) {
            case -1:
                // Premature EOF (everything should be terminated with \n)
                return new String(buffer, 0, offset);
            case '\n':
                // Found \n, remove \r if it is there:
                return new String(
                    buffer,
                    0,
                    '\r' == lastCh ? offset - 1 : offset);
            }
            // Line length exceeded:
            if (offset >= buffer.length) {
                return null;
            }
            buffer[offset++] = (byte)ch;
            lastCh = ch;
        }
    }
}
