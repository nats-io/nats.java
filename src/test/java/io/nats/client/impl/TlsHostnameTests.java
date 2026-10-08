// Copyright 2026 The NATS Authors
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

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.Options.HostnameResolveMode;
import io.nats.client.support.NatsInetAddress;
import io.nats.client.support.NatsInetAddressProvider;
import io.nats.client.support.NatsUri;
import io.nats.client.support.SSLUtils;
import io.nats.client.support.ssl.DiagnosticSslContext;
import io.nats.client.support.ssl.SniTestServer;
import io.nats.client.support.ssl.TrustCheckEvent;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The hostname a server is configured with must reach the TLS handshake as the server name (SNI)
 * in every hostname resolution mode, including the modes that resolve the hostname to ip addresses
 * before connecting. An endpoint that selects its certificate by SNI, like a TLS terminating proxy
 * or ingress, otherwise serves its default certificate.
 * <p>
 * The matrix runs every mode on a direct connection, and {@link HostnameResolveMode#Unresolved} additionally
 * through a loopback HTTP CONNECT tunnel, where it also checks that the proxy received the hostname and not an ip.
 * Each execution also checks whether the server pool was asked to resolve, which only the resolving modes do.
 */
public class TlsHostnameTests {
    private static final String SINGLE_LABEL_HOST = "nats";

    private static SniTestServer.Certificates dottedCertificates;
    private static SniTestServer.Certificates singleLabelCertificates;

    @BeforeAll
    public static void beforeAll() throws Exception {
        dottedCertificates = new SniTestServer.Certificates(SniTestServer.HOST);
        singleLabelCertificates = new SniTestServer.Certificates(SINGLE_LABEL_HOST);
        // HappyEyeballs resolves through NatsInetAddress instead of the server pool.
        // Map the test hostnames to loopback there so no DNS is involved.
        NatsInetAddress.setProvider(new NatsInetAddressProvider() {
            @Override
            public InetAddress[] getAllByName(String host) throws UnknownHostException {
                return isTestHost(host) ? new InetAddress[]{loopback(host)} : InetAddress.getAllByName(host);
            }

            @Override
            public InetAddress getByName(String host) throws UnknownHostException {
                return isTestHost(host) ? loopback(host) : InetAddress.getByName(host);
            }
        });
    }

    @AfterAll
    public static void afterAll() {
        NatsInetAddress.setProvider(null);
    }

    private static boolean isTestHost(String host) {
        return SniTestServer.HOST.equals(host) || SINGLE_LABEL_HOST.equals(host);
    }

    private static InetAddress loopback(String host) throws UnknownHostException {
        return InetAddress.getByAddress(host, new byte[]{127, 0, 0, 1});
    }

    /** every mode direct, plus Unresolved through a proxy: (mode, tlsFirst, proxied) */
    static Stream<Arguments> allModes() {
        Stream<Arguments> direct = Stream.of(HostnameResolveMode.values())
            .flatMap(mode -> Stream.of(Arguments.of(mode, false, false), Arguments.of(mode, true, false)));
        Stream<Arguments> proxied = Stream.of(
            Arguments.of(HostnameResolveMode.Unresolved, false, true),
            Arguments.of(HostnameResolveMode.Unresolved, true, true));
        return Stream.concat(direct, proxied);
    }

    private static Options.Builder options(SniTestServer server, String host, DiagnosticSslContext context, HostnameResolveMode mode, boolean tlsFirst) {
        return options(server, host, context, mode, tlsFirst, new AtomicBoolean());
    }

    private static Options.Builder options(SniTestServer server, String host, DiagnosticSslContext context, HostnameResolveMode mode, boolean tlsFirst, AtomicBoolean poolAsked) {
        Options.Builder builder = new Options.Builder()
            .server("tls://" + host + ":" + server.port())
            .serverPool(new NatsServerPool() {
                @Override
                public List<String> resolveHostToIps(String hostToResolve, boolean maxOneResult, boolean includeIPV6) {
                    poolAsked.set(true);
                    assertEquals(host, hostToResolve);
                    return Collections.singletonList("127.0.0.1");
                }
            })
            .sslContext(context)
            .hostnameResolveMode(mode)
            .noRandomize()
            .noReconnect()
            .connectionTimeout(Duration.ofSeconds(2));
        if (tlsFirst) {
            builder.tlsFirst();
        }
        return builder;
    }

    private static void assertValidCertificateSelected(SniTestServer server, SniTestServer.Certificates certificates, DiagnosticSslContext context) throws Exception {
        assertEquals(certificates.host, server.requestedName.get(2, TimeUnit.SECONDS));
        assertEquals(certificates.valid, server.selectedCertificate.get(2, TimeUnit.SECONDS));
        assertFalse(context.getTrustCheckEvents().isEmpty());
        for (TrustCheckEvent event : context.getTrustCheckEvents()) {
            assertTrue(event.trusted);
        }
    }

    @ParameterizedTest
    @MethodSource("allModes")
    public void hostnameIsSentAsSniInEveryMode(HostnameResolveMode mode, boolean tlsFirst, boolean proxied) throws Exception {
        connectAndVerify(dottedCertificates, mode, tlsFirst, proxied);
    }

    @ParameterizedTest
    @MethodSource("allModes")
    public void singleLabelHostnameIsSentAsSniInEveryMode(HostnameResolveMode mode, boolean tlsFirst, boolean proxied) throws Exception {
        // The JSSE does not derive SNI from a single label peer host on its own, so this
        // only passes because the data port sets the server name explicitly.
        connectAndVerify(singleLabelCertificates, mode, tlsFirst, proxied);
    }

    private static void connectAndVerify(SniTestServer.Certificates certificates, HostnameResolveMode mode, boolean tlsFirst, boolean proxied) throws Exception {
        DiagnosticSslContext context = certificates.clientContext();
        AtomicBoolean poolAsked = new AtomicBoolean();
        try (SniTestServer server = new SniTestServer(certificates, tlsFirst, 0);
             LoopbackConnectProxy proxy = proxied ? new LoopbackConnectProxy() : null) {
            Options.Builder builder = options(server, certificates.host, context, mode, tlsFirst, poolAsked);
            if (proxy != null) {
                builder.proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("127.0.0.1", proxy.port())));
            }
            try (Connection nc = Nats.connect(builder.build())) {
                nc.flush(Duration.ofSeconds(2));
                assertValidCertificateSelected(server, certificates, context);
                // only the resolving modes ask the pool; Unresolved and HappyEyeballs must not
                assertEquals(mode.resolve, poolAsked.get());
                if (proxy != null) {
                    // Unresolved means the client did not resolve: the proxy must have received the name, not an ip
                    assertEquals(Collections.singletonList(certificates.host + ":" + server.port()), proxy.receivedTargets);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void explicitIpSendsNoSniAndGetsTheDefaultCertificate(boolean tlsFirst) throws Exception {
        DiagnosticSslContext context = dottedCertificates.clientContext();
        try (SniTestServer server = new SniTestServer(dottedCertificates, tlsFirst, 0)) {
            Options.Builder builder = new Options.Builder()
                .server("tls://127.0.0.1:" + server.port())
                .sslContext(context)
                .noReconnect()
                .connectionTimeout(Duration.ofSeconds(2));
            if (tlsFirst) {
                builder.tlsFirst();
            }
            Options options = builder.build();
            assertThrows(IOException.class, () -> Nats.connect(options));
            assertEquals("", server.requestedName.get(2, TimeUnit.SECONDS));
            assertEquals(dottedCertificates.expired, server.selectedCertificate.get(2, TimeUnit.SECONDS));
            assertEquals(1, context.getTrustCheckEvents().size());
            TrustCheckEvent event = context.getTrustCheckEvents().get(0);
            assertFalse(event.trusted);
            assertEquals(dottedCertificates.expired.getNotAfter(), event.chain[0].getNotAfter());
            Throwable cause = event.failure;
            while (cause != null && !(cause instanceof CertificateExpiredException)) {
                cause = cause.getCause();
            }
            assertNotNull(cause, "Expected CertificateExpiredException: " + event.failure);
        }
    }

    @Test
    public void dataPortIsGivenTheUnresolvedUri() throws Exception {
        RecordingDataPort.reset();
        DiagnosticSslContext context = dottedCertificates.clientContext();
        try (SniTestServer server = new SniTestServer(dottedCertificates, false, 0);
             Connection nc = Nats.connect(options(server, SniTestServer.HOST, context, HostnameResolveMode.ResolveToAll, false)
                 .dataPortType(RecordingDataPort.class.getName())
                 .build())) {
            nc.flush(Duration.ofSeconds(2));
            assertNotNull(RecordingDataPort.resolved);
            assertNotNull(RecordingDataPort.unresolved);
            assertEquals("127.0.0.1", RecordingDataPort.resolved.getHost());
            assertTrue(RecordingDataPort.resolved.hostIsIpAddress());
            assertEquals(SniTestServer.HOST, RecordingDataPort.unresolved.getHost());
            assertEquals(server.port(), RecordingDataPort.unresolved.getPort());
            assertEquals(SniTestServer.HOST, RecordingDataPort.tlsHostSeen);
            assertValidCertificateSelected(server, dottedCertificates, context);
        }
    }

    @Test
    public void dataPortWithoutTheUnresolvedUriStillSendsAnUnresolvedHostname() throws Exception {
        // A data port implementing only the older connect gets the uri after resolution.
        // In a non resolving mode that uri still has the hostname, which must still be sent.
        RecordingDataPort.reset();
        DiagnosticSslContext context = dottedCertificates.clientContext();
        try (SniTestServer server = new SniTestServer(dottedCertificates, false, 0);
             Connection nc = Nats.connect(options(server, SniTestServer.HOST, context, HostnameResolveMode.HappyEyeballs, false)
                 .dataPortType(LegacyConnectDataPort.class.getName())
                 .build())) {
            nc.flush(Duration.ofSeconds(2));
            assertNull(RecordingDataPort.unresolved);
            assertEquals(SniTestServer.HOST, RecordingDataPort.resolved.getHost());
            assertEquals(SniTestServer.HOST, RecordingDataPort.tlsHostSeen);
            assertValidCertificateSelected(server, dottedCertificates, context);
        }
    }

    /**
     * HTTP CONNECT proxy on loopback that tunnels to 127.0.0.1 at the requested port whatever host was requested,
     * so no DNS is involved, and records the targets it was asked for.
     */
    static class LoopbackConnectProxy implements AutoCloseable {
        final List<String> receivedTargets = new CopyOnWriteArrayList<>();
        private final ServerSocket listener;
        private final ExecutorService executor = Executors.newCachedThreadPool();
        private volatile boolean closed;

        LoopbackConnectProxy() throws IOException {
            listener = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"));
            executor.submit(() -> {
                try {
                    while (!closed) {
                        Socket client = listener.accept();
                        executor.submit(() -> tunnel(client));
                    }
                }
                catch (IOException ignore) {
                    // closed
                }
            });
        }

        int port() {
            return listener.getLocalPort();
        }

        private void tunnel(Socket client) {
            try {
                InputStream in = client.getInputStream();
                OutputStream out = client.getOutputStream();
                String requestLine = readLine(in);
                if (requestLine == null || !requestLine.startsWith("CONNECT ")) {
                    client.close();
                    return;
                }
                String target = requestLine.split("\\s+")[1];
                receivedTargets.add(target);
                String line;
                while ((line = readLine(in)) != null && !line.isEmpty()) {
                    // consume headers
                }
                int targetPort = Integer.parseInt(target.substring(target.lastIndexOf(':') + 1));
                Socket remote = new Socket();
                remote.connect(new InetSocketAddress("127.0.0.1", targetPort), 2000);
                out.write("HTTP/1.1 200 Connection Established\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                executor.submit(() -> pump(remote, client));
                pump(client, remote);
            }
            catch (IOException ignore) {
                try { client.close(); } catch (IOException ignoreClose) { /* closing */ }
            }
        }

        private static void pump(Socket from, Socket to) {
            byte[] buffer = new byte[8192];
            try {
                InputStream in = from.getInputStream();
                OutputStream out = to.getOutputStream();
                int read;
                while ((read = in.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                    out.flush();
                }
            }
            catch (IOException ignore) {
                // one side closed
            }
            finally {
                try { to.close(); } catch (IOException ignore) { /* closing */ }
                try { from.close(); } catch (IOException ignore) { /* closing */ }
            }
        }

        private static String readLine(InputStream in) throws IOException {
            StringBuilder sb = new StringBuilder();
            int ch;
            while ((ch = in.read()) != -1) {
                if (ch == '\r') {
                    continue;
                }
                if (ch == '\n') {
                    return sb.toString();
                }
                sb.append((char) ch);
            }
            return sb.length() == 0 ? null : sb.toString();
        }

        @Override
        public void close() throws Exception {
            closed = true;
            listener.close();
            executor.shutdownNow();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IOException("Loopback CONNECT proxy did not stop");
            }
        }
    }

    // ---- tls hostname verification ----

    private static Options.Builder verifying(SniTestServer server, String host, SSLContext context, boolean tlsFirst, List<Throwable> errors) {
        Options.Builder builder = new Options.Builder()
            .server("tls://" + host + ":" + server.port())
            .serverPool(new NatsServerPool() {
                @Override
                public List<String> resolveHostToIps(String hostToResolve, boolean maxOneResult, boolean includeIPV6) {
                    return Collections.singletonList("127.0.0.1");
                }
            })
            .sslContext(context)
            .tlsVerifyHostname()
            .noRandomize()
            .noReconnect()
            .connectionTimeout(Duration.ofSeconds(2))
            .errorListener(new ErrorListener() {
                @Override
                public void exceptionOccurred(Connection conn, Exception exp) {
                    errors.add(exp);
                }
            });
        if (tlsFirst) {
            builder.tlsFirst();
        }
        return builder;
    }

    private static void assertIdentityFailure(List<Throwable> errors, String expectedInMessage) {
        assertFalse(errors.isEmpty(), "expected a connect failure to be reported");
        boolean found = false;
        for (Throwable t : errors) {
            for (Throwable c = t; c != null; c = c.getCause()) {
                if (c instanceof CertificateException && !(c instanceof CertificateExpiredException)
                    && c.getMessage() != null && c.getMessage().contains(expectedInMessage)) {
                    found = true;
                }
            }
        }
        assertTrue(found, "expected a CertificateException mentioning '" + expectedInMessage + "' in " + errors);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void verifyHostnameAcceptsACertificateForTheHostname(boolean tlsFirst) throws Exception {
        // default mode: the hostname was resolved to 127.0.0.1, the check is against the hostname, not the ip
        DiagnosticSslContext context = dottedCertificates.clientContext();
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        try (SniTestServer server = new SniTestServer(dottedCertificates, tlsFirst, 0);
             Connection nc = Nats.connect(verifying(server, SniTestServer.HOST, context, tlsFirst, errors).build())) {
            nc.flush(Duration.ofSeconds(2));
            assertValidCertificateSelected(server, dottedCertificates, context);
            assertTrue(errors.isEmpty(), errors.toString());
        }
    }

    @Test
    public void verifyHostnameRejectsACertificateForAnotherName() throws Exception {
        // the server always presents a valid, trusted certificate, but for other.test
        SniTestServer.Certificates otherName = new SniTestServer.Certificates("other.test");
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        try (SniTestServer server = new SniTestServer(otherName, false, 0, true)) {
            Options verifying = verifying(server, SniTestServer.HOST, otherName.clientContext(), false, errors).build();
            assertThrows(IOException.class, () -> Nats.connect(verifying));
            assertEquals(SniTestServer.HOST, server.requestedName.get(2, TimeUnit.SECONDS));
            assertIdentityFailure(errors, SniTestServer.HOST);
        }
        // the same server and certificate are accepted when verification is off
        try (SniTestServer server = new SniTestServer(otherName, false, 0, true);
             Connection nc = Nats.connect(options(server, SniTestServer.HOST, otherName.clientContext(), HostnameResolveMode.ResolveToAll, false).build())) {
            nc.flush(Duration.ofSeconds(2));
            assertEquals(otherName.valid, server.selectedCertificate.get(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void verifyHostnameChecksAnIpLiteralAgainstIpSubjectAlternativeNames() throws Exception {
        // with an ip SAN for 127.0.0.1 the connection is accepted
        SniTestServer.Certificates withIpSan = new SniTestServer.Certificates(SniTestServer.HOST, true);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        try (SniTestServer server = new SniTestServer(withIpSan, false, 0, true);
             Connection nc = Nats.connect(verifying(server, "127.0.0.1", withIpSan.clientContext(), false, errors).build())) {
            nc.flush(Duration.ofSeconds(2));
            assertEquals("", server.requestedName.get(2, TimeUnit.SECONDS));
            assertEquals(withIpSan.valid, server.selectedCertificate.get(2, TimeUnit.SECONDS));
            assertTrue(errors.isEmpty(), errors.toString());
        }
        // without it the connection is rejected
        SniTestServer.Certificates withoutIpSan = new SniTestServer.Certificates(SniTestServer.HOST, false);
        errors.clear();
        try (SniTestServer server = new SniTestServer(withoutIpSan, false, 0, true)) {
            Options verifying = verifying(server, "127.0.0.1", withoutIpSan.clientContext(), false, errors).build();
            assertThrows(IOException.class, () -> Nats.connect(verifying));
            assertIdentityFailure(errors, "127.0.0.1");
        }
    }

    @Test
    public void verifyHostnameAppliesUnderTheTrustAllContext() throws Exception {
        // opentls trusts any chain; the JDK wraps its plain X509TrustManager and still checks the name
        SniTestServer.Certificates otherName = new SniTestServer.Certificates("other.test");
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        try (SniTestServer server = new SniTestServer(otherName, false, 0, true)) {
            Options verifying = verifying(server, SniTestServer.HOST, SSLUtils.createTrustAllTlsContext(), false, errors).build();
            assertThrows(IOException.class, () -> Nats.connect(verifying));
            assertIdentityFailure(errors, SniTestServer.HOST);
        }
    }

    public static class RecordingDataPort extends SocketDataPort {
        static volatile NatsUri resolved;
        static volatile NatsUri unresolved;
        static volatile String tlsHostSeen;

        static void reset() {
            resolved = null;
            unresolved = null;
            tlsHostSeen = null;
        }

        @Override
        public void connect(@NonNull NatsConnection conn, @NonNull NatsUri nuri, @NonNull NatsUri unresolvedUri, long timeoutNanos) throws IOException {
            resolved = nuri;
            unresolved = unresolvedUri;
            super.connect(conn, nuri, unresolvedUri, timeoutNanos);
        }

        @Override
        public void upgradeToSecure() throws IOException {
            tlsHostSeen = tlsHost;
            super.upgradeToSecure();
        }
    }

    public static class LegacyConnectDataPort extends RecordingDataPort {
        @Override
        public void connect(@NonNull NatsConnection conn, @NonNull NatsUri nuri, @NonNull NatsUri unresolvedUri, long timeoutNanos) throws IOException {
            // behave like an implementation that predates the unresolved uri
            connect(conn, nuri, timeoutNanos);
        }

        @Override
        public void connect(@NonNull NatsConnection conn, @NonNull NatsUri nuri, long timeoutNanos) throws IOException {
            resolved = nuri;
            super.connect(conn, nuri, timeoutNanos);
        }
    }
}
