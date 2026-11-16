// Copyright 2015-2026 The NATS Authors
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
import io.nats.client.Options.HostnameResolveMode;
import io.nats.client.support.HappyEyeballsConnector;
import io.nats.client.support.NatsInetAddress;
import io.nats.client.support.NatsUri;
import io.nats.client.support.WebSocket;
import org.jspecify.annotations.NonNull;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.nats.client.support.NatsConstants.SECURE_WEBSOCKET_PROTOCOL;

/**
 * This class is not thread-safe.  Caller must ensure thread safety.
 */
@SuppressWarnings("ClassEscapesDefinedScope") // NatsConnection
public class SocketDataPort implements DataPort {

    protected NatsConnection connection;

    protected String host;
    protected int port;
    protected String tlsHost;
    // where tlsHost came from, named in the error when the certificate is not issued for it; see connect and nameCheckFailure
    protected String tlsHostSource;
    // tlsHost is an ip address: the certificate is checked against it as one, and no server name is sent
    protected boolean tlsHostIsIpAddress;
    // written by the thread that connects, read by the thread that closes an attempt at its deadline or on close()
    protected volatile Socket socket;
    // set by close() and forceClose(), read by the thread that connects: a connect that completes after the port was
    // closed, as when an attempt was abandoned at its deadline before the socket was assigned, closes the socket it
    // got and fails, instead of leaving an open socket that nothing closes
    protected volatile boolean closed;
    protected boolean isSecure = false;

    protected InputStream in;
    protected OutputStream out;

    static final String TLS_HOST_FROM_URL = "the host of its url";
    static final String TLS_HOST_FROM_OPTIONS = "the tlsServerName of the options";
    static final String TLS_HOST_FROM_REPORTING_SERVER = "the hostname of the server that reported it, this server having been discovered as an ip address";

    @Deprecated
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
    public void connect(@NonNull NatsConnection conn, @NonNull NatsUri nuri, @NonNull NatsUri unresolvedUri, long timeoutNanos) throws IOException {
        // The unresolved uri is the server as configured or discovered, before any hostname resolution.
        // Its tls host is its hostname, or the hostname saved on it when it was discovered as a bare
        // ip address from a server that had one. That is the server name to present during the TLS
        // handshake, whatever the hostname resolution mode turned the host of nuri into.
        String name = unresolvedUri.getTlsHost();
        if (!unresolvedUri.hostIsIpAddress()) {
            tlsHost = name;
            tlsHostSource = TLS_HOST_FROM_URL;
        }
        else if (!name.equals(unresolvedUri.getHost())) {
            tlsHost = name;
            tlsHostSource = TLS_HOST_FROM_REPORTING_SERVER;
        }
        connect(conn, nuri, timeoutNanos);
    }

    @Override
    public void connect(@NonNull NatsConnection conn, @NonNull NatsUri nuri, long timeoutNanos) throws IOException {
        connection = conn;
        Options options = connection.getOptions();
        long timeout = timeoutNanos / 1_000_000; // convert to millis
        host = nuri.getHost();
        port = nuri.getPort();
        if (options.getTlsServerName() != null) {
            // ADR-40, certificate verification: the name the user set is the name of every server, configured or
            // discovered, over the host of the url and over the name a discovered ip address inherited.
            tlsHost = options.getTlsServerName();
            tlsHostSource = TLS_HOST_FROM_OPTIONS;
        }
        else if (tlsHost == null && !nuri.hostIsIpAddress()) {
            // connect was called without the unresolved uri, or the host was not resolved.
            tlsHost = host;
            tlsHostSource = TLS_HOST_FROM_URL;
        }
        if (tlsHost != null) {
            // ADR-40, certificate verification: one trailing dot is stripped from the name verified against and sent
            // as the server name. RFC 6066 has no trailing dot in SNI, nats-server refuses a handshake whose name ends
            // in one, and a certificate carries the name without it. The host is dialed as written.
            if (tlsHost.length() > 1 && tlsHost.endsWith(".")) {
                tlsHost = tlsHost.substring(0, tlsHost.length() - 1);
            }
            // A name that is an ip address is verified as one, against the certificate's ip subject alternative names,
            // and no server name is sent: RFC 6066 does not allow an ip address in SNI. A configured ip address has
            // no tlsHost and is verified the same way by the provider; this is a tlsServerName that is an ip address.
            tlsHostIsIpAddress = NatsUri.isIpAddress(tlsHost);
            if (tlsHostIsIpAddress && tlsHost.startsWith("[") && tlsHost.endsWith("]")) {
                tlsHost = tlsHost.substring(1, tlsHost.length() - 1);
            }
        }

        try {
            HostnameResolveMode mode = options.hostnameResolveMode();
            if (mode == HostnameResolveMode.HappyEyeballs) {
                socket = HappyEyeballsConnector.connect(
                    options.getExecutor(),
                    () -> createSocket(options),
                    host, port, (int) timeout
                );
            }
            else {
                socket = createSocket(options);
                InetSocketAddress inetSocketAddress;
                if (mode == HostnameResolveMode.Unresolved && !nuri.hostIsIpAddress()) {
                    if (options.getProxy() == null) {
                        // There is no proxy to hand the hostname to, and a plain socket rejects an unresolved
                        // address. Resolve one address here, at connect time, through NatsInetAddress, the way
                        // InetSocketAddress(String, int) would have. host and tlsHost keep the hostname.
                        inetSocketAddress = new InetSocketAddress(NatsInetAddress.getByName(host), port);
                    }
                    else {
                        // The proxy receives the hostname and resolves it.
                        inetSocketAddress = InetSocketAddress.createUnresolved(host, port);
                    }
                }
                else {
                    inetSocketAddress = new InetSocketAddress(host, port);
                }
                socket.connect(inetSocketAddress, (int) timeout);
            }
            if (closed) {
                throw new IOException("closed during connect"); // the catch below closes the socket
            }

            if (options.getSocketReadTimeoutMillis() > 0) {
                socket.setSoTimeout(options.getSocketReadTimeoutMillis());
            }

            if (options.getSocketSoLinger() > 0) {
                socket.setSoLinger(true, options.getSocketSoLinger());
            }

            if (options.getReceiveBufferSize() > 0) {
                socket.setReceiveBufferSize(options.getReceiveBufferSize());
            }

            if (options.getSendBufferSize() > 0) {
                socket.setSendBufferSize(options.getSendBufferSize());
            }

            if (nuri.isWebsocket()) {
                if (SECURE_WEBSOCKET_PROTOCOL.equalsIgnoreCase(nuri.getScheme())) {
                    upgradeToSecure();
                }
                try {
                    socket = new WebSocket(socket, host, options.getHttpRequestInterceptors(), nuri.getUri().getPath());
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

        String peerHost = tlsHost == null ? host : tlsHost;
        SSLSocket sslSocket = (SSLSocket) factory.createSocket(socket, peerHost, port, true);
        sslSocket.setUseClientMode(true);

        SSLParameters sslParameters = sslSocket.getSSLParameters();
        // ADR-40, TLS versions: never negotiate below TLS 1.2 and never fall back to a lower version, whatever the
        // context enables
        sslParameters.setProtocols(tlsProtocolsAtOrAbove12(sslParameters.getProtocols()));
        boolean verifyHostname = options.isTlsVerifyHostname();
        if (tlsHost != null) {
            if (tlsHostIsIpAddress) {
                // no server name for an ip address; an empty list sends no SNI extension
                sslParameters.setServerNames(Collections.<SNIServerName>emptyList());
            }
            else {
                // Present the hostname to the server as the TLS server name (SNI) in every hostname resolution mode.
                // Setting it explicitly also covers names the provider would not derive from the peer host on its own,
                // for instance a single label hostname like a kubernetes service name.
                try {
                    sslParameters.setServerNames(Collections.<SNIServerName>singletonList(new SNIHostName(tlsHost)));
                }
                catch (IllegalArgumentException e) {
                    // not a legal SNI host name. Leave the server name to the provider's default behavior for the peer host.
                }
            }
        }
        if (verifyHostname) {
            // The trust manager checks the certificate against the server name, or the peer host when there is none.
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
        }
        sslSocket.setSSLParameters(sslParameters);

        final CompletableFuture<Void> waitForHandshake = new CompletableFuture<>();
        final HandshakeCompletedListener hcl = (evt) -> waitForHandshake.complete(null);

        sslSocket.addHandshakeCompletedListener(hcl);
        try {
            sslSocket.startHandshake();
        }
        catch (SSLHandshakeException e) {
            throw nameCheckFailure(e);
        }

        try {
            awaitHandshake(waitForHandshake, timeout);
        }
        finally {
            sslSocket.removeHandshakeCompletedListener(hcl);
        }

        socket = sslSocket;
        in = sslSocket.getInputStream();
        out = sslSocket.getOutputStream();
        isSecure = true;
    }

    /**
     * Of the protocols a context enables, the ones at TLS 1.2 or above: the client never negotiates TLS 1.0, TLS 1.1 or
     * SSL, and never falls back to them (ADR-40, TLS versions; RFC 8996 and BCP 195). A context that enables nothing
     * at or above TLS 1.2 gets no protocol, and its handshake fails.
     * <p>This is an allow-list on the name form {@code TLSv1.N} with N at least 2, chosen over a deny-list of the four
     * names below the floor, {@code SSLv2Hello}, {@code SSLv3}, {@code TLSv1} and {@code TLSv1.1}: a protocol name the
     * client does not know is dropped rather than kept, the safer reading against an unknown weak name. A TLS version
     * named in another form is kept once the client learns it.</p>
     * @param enabled the protocols the socket enables
     * @return the ones kept
     */
    static String[] tlsProtocolsAtOrAbove12(String[] enabled) {
        List<String> kept = new ArrayList<>();
        for (String protocol : enabled) {
            if (protocol.startsWith("TLSv1.")) {
                try {
                    if (Integer.parseInt(protocol.substring("TLSv1.".length())) >= 2) {
                        kept.add(protocol);
                    }
                }
                catch (NumberFormatException ignore) {
                    // not a version the client knows: not kept
                }
            }
        }
        return kept.toArray(new String[0]);
    }

    /**
     * Waits for the handshake-completed notification of the handshake that startHandshake ran. ADR-40, TLS: an
     * upgrade whose handshake is not confirmed fails the attempt. It never returns with the plaintext streams still
     * in place, so no CONNECT and no credential go out unprotected; before, the failure was reported and the upgrade
     * returned.
     * @param handshakeCompleted completed by the socket's handshake-completed listener
     * @param timeout how long to wait, the connection timeout
     * @throws IOException when the notification did not arrive within the timeout or the wait failed
     */
    protected void awaitHandshake(CompletableFuture<Void> handshakeCompleted, Duration timeout) throws IOException {
        try {
            handshakeCompleted.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        catch (TimeoutException e) {
            throw new IOException("TLS handshake not confirmed within the connection timeout", e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("TLS handshake not confirmed", e);
        }
        catch (ExecutionException e) {
            throw new IOException("TLS handshake not confirmed", e.getCause());
        }
    }

    /**
     * ADR-40, certificate verification: when the name check failed, the error states the name the client expected and
     * where it came from, points at tlsServerName and, for a server discovered from the cluster, at the remedy on the
     * server side, client_advertise. It never suggests turning verification off. The JDK's CertificateException does
     * not carry the names the certificate presented, so the error cannot state them. The failure is recognized by the
     * text of the JDK's own identity check (SunJSSE); a failure from another provider, and any other handshake
     * failure, is returned unchanged.
     * @param e the handshake failure
     * @return the failure to throw: a wrapped one when the name check failed, otherwise e itself
     */
    protected SSLHandshakeException nameCheckFailure(SSLHandshakeException e) {
        CertificateException check = null;
        for (Throwable t = e.getCause(); t != null && check == null; t = t.getCause()) {
            if (t instanceof CertificateException && isNameCheckMessage(t.getMessage())) {
                check = (CertificateException) t;
            }
        }
        if (check == null) {
            return e;
        }
        String expected = tlsHost == null ? host : tlsHost;
        String source = tlsHostSource == null ? TLS_HOST_FROM_URL : tlsHostSource;
        SSLHandshakeException wrapped = new SSLHandshakeException("TLS certificate name check failed for " + host + ":" + port
            + ". The certificate is not issued for " + expected + ", which is " + source
            + ". The JDK reported: \"" + check.getMessage() + "\"."
            + " Set tlsServerName to a name the certificate carries, or reissue the certificate for " + expected
            + ". A server discovered from the cluster can advertise a name its certificate carries, client_advertise on nats-server.");
        wrapped.initCause(e);
        return wrapped;
    }

    // The messages of the JDK's identity check (sun.security.util.HostnameChecker): "No subject alternative names present",
    // "No subject alternative DNS name matching x found.", "No subject alternative names matching IP address x found",
    // "No name matching x found". A chain failure has other text and is not a name check failure.
    private static boolean isNameCheckMessage(String message) {
        return message != null && (message.startsWith("No subject alternative") || message.startsWith("No name matching"));
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
        closed = true; // before the socket is read: a connect that assigns the socket after this sees the flag
        if (socket != null) {
            socket.close();
        }
    }

    @Override
    public void forceClose() throws IOException {
        closed = true;
        // socket is null while a connect has not assigned it yet, as under HappyEyeballs until the race is won
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
