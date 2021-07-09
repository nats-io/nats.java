package io.nats.client.support;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import io.nats.client.HttpRequest;

import static java.nio.charset.StandardCharsets.UTF_8;

public class WebSocket extends Socket {
    private static final int MAX_LINE_LEN = 8192;
    private static final int MAX_HTTP_HEADERS = 100;
    private static final String WEBSOCKET_RESPONSE_LINE =
        "HTTP/1.1 101 Switching Protocols";

    private Socket wrap;
    private WebsocketInputStream in;
    private WebsocketOutputStream out;

    public WebSocket(Socket wrap, String host, List<Consumer<HttpRequest>> interceptors) throws IOException {
        this.wrap = wrap;
        handshake(wrap, host, interceptors);
        this.in = new WebsocketInputStream(wrap.getInputStream());
        this.out = new WebsocketOutputStream(wrap.getOutputStream(), true);
    }

    private static void handshake(Socket socket, String host, List<Consumer<HttpRequest>> interceptors) throws IOException {
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        HttpRequest request = new HttpRequest();

        // The value of this header field MUST be a
        // nonce consisting of a randomly selected 16-byte value that has
        // been base64-encoded
        byte[] keyBytes = new byte[16];
        new SecureRandom().nextBytes(keyBytes);
        String key = Base64.getEncoder().encodeToString(keyBytes);

        request.getHeaders()
            .add("Host", host)
            .add("Upgrade", "websocket")
            .add("Connection", "Upgrade")
            .add("Sec-WebSocket-Key", key)
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

    @Override
    public InputStream getInputStream() throws IOException {
        return in;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return out;
    }

    @Override
    public void connect(SocketAddress addr) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void connect(SocketAddress addr, int port) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bind(SocketAddress addr) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketChannel getChannel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InetAddress getInetAddress() {
        return wrap.getInetAddress();
    }

    @Override
    public InetAddress getLocalAddress() {
        return wrap.getLocalAddress();
    }

    @Override
    public int getPort() {
        return wrap.getPort();
    }

    @Override
    public int getLocalPort() {
        return wrap.getLocalPort();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        return wrap.getRemoteSocketAddress();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        return wrap.getLocalSocketAddress();
    }

    @Override
    public void setTcpNoDelay(boolean on) throws SocketException {
        wrap.setTcpNoDelay(on);
    }

    @Override
    public boolean getTcpNoDelay() throws SocketException {
        return wrap.getTcpNoDelay();
    }

    @Override
    public void setSoLinger(boolean on, int linger) throws SocketException {
        wrap.setSoLinger(on, linger);
    }

    @Override
    public int getSoLinger() throws SocketException {
        return wrap.getSoLinger();
    }

    @Override
    public void sendUrgentData(int data) throws IOException {
        wrap.sendUrgentData(data);
    }

    @Override
    public void setOOBInline(boolean on) throws SocketException {
        wrap.setOOBInline(on);
    }

    @Override
    public boolean getOOBInline() throws SocketException {
        return wrap.getOOBInline();
    }

    @Override
    public void setSoTimeout(int timeout) throws SocketException {
        wrap.setSoTimeout(timeout);
    }

    @Override
    public int getSoTimeout() throws SocketException {
        return wrap.getSoTimeout();
    }

    @Override
    public void setSendBufferSize(int size) throws SocketException {
        wrap.setSendBufferSize(size);
    }

    @Override
    public int getSendBufferSize() throws SocketException {
        return wrap.getSendBufferSize();
    }

    @Override
    public void setReceiveBufferSize(int size) throws SocketException {
        wrap.setReceiveBufferSize(size);
    }

    @Override
    public int getReceiveBufferSize() throws SocketException {
        return wrap.getReceiveBufferSize();
    }

    @Override
    public void setKeepAlive(boolean on) throws SocketException {
        wrap.setKeepAlive(on);
    }

    @Override
    public boolean getKeepAlive() throws SocketException {
        return wrap.getKeepAlive();
    }

    @Override
    public void setTrafficClass(int tc) throws SocketException {
        wrap.setTrafficClass(tc);
    }

    @Override
    public int getTrafficClass() throws SocketException {
        return wrap.getTrafficClass();
    }

    @Override
    public void setReuseAddress(boolean on) throws SocketException {
        wrap.setReuseAddress(on);
    }

    @Override
    public boolean getReuseAddress() throws SocketException {
        return wrap.getReuseAddress();
    }

    @Override
    public synchronized void close() throws IOException {
        // TODO: send websocket close:
        wrap.close();
    }

    @Override
    public void shutdownInput() throws IOException {
        wrap.shutdownInput();
    }

    @Override
    public void shutdownOutput() throws IOException {
        wrap.shutdownOutput();
    }

    @Override
    public boolean isConnected() {
        return wrap.isConnected();
    }

    @Override
    public boolean isBound() {
        return wrap.isBound();
    }

    @Override
    public boolean isClosed() {
        return wrap.isClosed();
    }

    @Override
    public boolean isInputShutdown() {
        return wrap.isInputShutdown();
    }

    @Override
    public boolean isOutputShutdown() {
        return wrap.isOutputShutdown();
    }

    @Override
    public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
        wrap.setPerformancePreferences(connectionTime, latency, bandwidth);
    }
}
