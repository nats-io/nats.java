package io.nats.client.support.ssl;

import javax.net.ssl.HandshakeCompletedEvent;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;

/**
 * An SSLSocketFactory wrapper that attaches a handshake listener to every
 * created socket, recording {@link HandshakeEvent} instances on completion.
 */
class RecordingSSLSocketFactory extends SSLSocketFactory {
    private final SSLSocketFactory delegate;
    private final List<HandshakeEvent> events;

    RecordingSSLSocketFactory(SSLSocketFactory delegate, List<HandshakeEvent> events) {
        this.delegate = delegate;
        this.events = events;
    }

    private Socket addListener(Socket socket) {
        if (socket instanceof SSLSocket) {
            ((SSLSocket) socket).addHandshakeCompletedListener(
                new HandshakeCompletedListener() {
                    @Override
                    public void handshakeCompleted(HandshakeCompletedEvent event) {
                        events.add(new HandshakeEvent(event));
                    }
                });
        }
        return socket;
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose)
        throws IOException {
        return addListener(delegate.createSocket(s, host, port, autoClose));
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return delegate.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return addListener(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
        throws IOException {
        return addListener(delegate.createSocket(host, port, localHost, localPort));
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        return addListener(delegate.createSocket(host, port));
    }

    @Override
    public Socket createSocket(InetAddress address, int port,
                               InetAddress localAddress, int localPort)
        throws IOException {
        return addListener(delegate.createSocket(address, port, localAddress, localPort));
    }
}
