package io.nats.client.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;

public class SocketNatsChannel implements NatsChannel {
    private String host;
    private int port;
    private Socket socket;

    private InputStream in;
    private OutputStream out;
    private boolean isOpen = true;

    public static NatsChannel connect(
        URI serverURI,
        Duration timeout)
        throws IOException
    {
        return new SocketNatsChannel(serverURI, timeout);
    }

    private SocketNatsChannel(URI uri, Duration timeoutDuration) throws IOException {
        long timeoutNanos = timeoutDuration.getNano();
        // Code copied from SocketDataPort.connect():
        try {
            long timeout = timeoutNanos / 1_000_000; // convert to millis
            this.host = uri.getHost();
            this.port = uri.getPort();

            this.socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(2 * 1024 * 1024);
            socket.setSendBufferSize(2 * 1024 * 1024);
            socket.connect(new InetSocketAddress(host, port), (int) timeout);

            in = socket.getInputStream();
            out = socket.getOutputStream();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        // FIXME: Slow!
        byte[] tmp = new byte[dst.remaining()];
        int length = in.read(tmp, 0, tmp.length);
        if (length < 0) {
            return length;
        }
        dst.put(tmp, 0, length);
        return length;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        socket.close();
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        // FIXME: Slow!
        byte[] tmp = new byte[src.remaining()];
        src.get(tmp);
        out.write(tmp, 0, tmp.length);
        return tmp.length;
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public void shutdownInput() throws IOException {
        socket.shutdownInput();
    }

    @Override
    public void flushOutput() throws IOException {
        out.flush();
    }
    
}
