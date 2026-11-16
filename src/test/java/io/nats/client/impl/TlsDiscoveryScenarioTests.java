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
import io.nats.client.ConnectionListener.Events;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.Subscription;
import io.nats.client.support.ssl.SniTestServer;
import io.nats.client.support.ssl.SniTestServer.Certificates;
import io.nats.client.support.ssl.SniTestServer.PemFiles;
import io.nats.client.support.ssl.SslTestingHelper;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests for the scenarios the TLS discovery and reconnect standard expects in each client's own suite
 * (ADR-40; standard X1: S02, S06, S16 to S18, S21, S25 to S27, and S09). A scenario about a cluster runs against two
 * real nats-servers; a scenario about bytes on the wire runs against {@link SniTestServer}. Two are covered elsewhere:
 * S09, url credentials carried to discovered servers, by {@code ReconnectTests.testUrlCredentialsReachDiscoveredServers};
 * S27, server-advertised ws:// and nats:// entries against a TLS client, by
 * {@code TLSConnectTests.testConnectUrlsEntriesWithASchemeAreSkippedAndNotDialed}. The last test is outside the
 * standard's list: a server advertised under a rooted name, against real servers.
 */
public class TlsDiscoveryScenarioTests extends TestBase {
    private static final String PLAIN_CONF = "src/test/resources/cluster_plain.conf";
    private static final String CERT = "src/test/resources/certs/server.pem";
    private static final String KEY = "src/test/resources/certs/key.pem";
    private static final String CERT_NO_IP = "src/test/resources/certs/server_noip.pem";
    private static final String KEY_NO_IP = "src/test/resources/certs/key_noip.pem";

    /**
     * records the protocol of every TLS session the connection negotiates, one per upgrade, and the server name the
     * connection sent in each
     */
    public static class RecordingDataPort extends SocketDataPort {
        static final List<String> protocols = new CopyOnWriteArrayList<>();
        static final List<String> serverNames = new CopyOnWriteArrayList<>();

        @Override
        public void upgradeToSecure() throws IOException {
            super.upgradeToSecure();
            SSLSocket tls = (SSLSocket) socket;
            protocols.add(tls.getSession().getProtocol());
            List<SNIServerName> names = tls.getSSLParameters().getServerNames();
            if (names != null) {
                for (SNIServerName name : names) {
                    serverNames.add(name instanceof SNIHostName ? ((SNIHostName) name).getAsciiName() : name.toString());
                }
            }
        }
    }

    /**
     * two nats-servers routed to each other, each advertising 127.0.0.1 and its port unless the second is given another
     * host to advertise, configured by inserts
     */
    private static final class Cluster implements AutoCloseable {
        final int aPort = NatsTestServer.nextPort();
        final int bPort = NatsTestServer.nextPort();
        final int aRoute = NatsTestServer.nextPort();
        final int bRoute = NatsTestServer.nextPort();
        final String bAdvertiseHost;
        final NatsTestServer a;
        final NatsTestServer b;
        private boolean aClosed;

        Cluster(Consumer<List<String>> aExtra, Consumer<List<String>> bExtra) throws Exception {
            this(aExtra, bExtra, "127.0.0.1");
        }

        Cluster(Consumer<List<String>> aExtra, Consumer<List<String>> bExtra, String bAdvertiseHost) throws Exception {
            this.bAdvertiseHost = bAdvertiseHost;
            List<String> aInserts = inserts(aPort, aRoute, bRoute, "127.0.0.1");
            aExtra.accept(aInserts);
            List<String> bInserts = inserts(bPort, bRoute, aRoute, bAdvertiseHost);
            bExtra.accept(bInserts);
            a = new NatsTestServer(PLAIN_CONF, aInserts.toArray(new String[0]), aPort, false);
            b = new NatsTestServer(PLAIN_CONF, bInserts.toArray(new String[0]), bPort, false);
        }

        private static List<String> inserts(int port, int listenPort, int routePort, String advertiseHost) {
            return new ArrayList<>(Arrays.asList(
                "client_advertise: \"" + advertiseHost + ":" + port + "\"",
                "cluster {",
                "name: scenarioCluster",
                "listen: localhost:" + listenPort,
                "routes = [",
                "nats-route://localhost:" + routePort,
                "]",
                "}"));
        }

        String aByName() {
            return "tls://localhost:" + aPort;
        }

        String bDiscovered() {
            return "tls://" + bAdvertiseHost + ":" + bPort;
        }

        void killA() throws Exception {
            if (!aClosed) {
                aClosed = true;
                a.close();
            }
        }

        @Override
        public void close() throws Exception {
            killA();
            b.close();
        }
    }

    private static Consumer<List<String>> tls(String certFile, String keyFile, boolean allowNonTls) {
        return inserts -> {
            inserts.add("tls { cert_file: \"" + certFile + "\", key_file: \"" + keyFile + "\", timeout: 2 }");
            if (allowNonTls) {
                inserts.add("allow_non_tls: true");
            }
        };
    }

    private static Consumer<List<String>> plain() {
        return inserts -> {};
    }

    // connects to the seed with a good context and waits until it has reported the other member. The seed advertises
    // itself as 127.0.0.1 and its port, another entry than the configured localhost one, so the pool reaching two
    // entries is not the sign; the other member's entry is
    private static void waitForClusterToForm(Cluster cluster, SSLContext context) throws Exception {
        Options options = new Options.Builder()
            .server(cluster.aByName())
            .sslContext(context)
            .build();
        try (Connection nc = standardConnectionWait(Nats.connect(options))) {
            long deadline = System.currentTimeMillis() + 10_000;
            while (!nc.getServers().contains(cluster.bDiscovered()) && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }
            assertTrue(nc.getServers().contains(cluster.bDiscovered()), "cluster did not form: " + nc.getServers());
        }
    }

    private static Options.Builder client(Cluster cluster, SSLContext context, ListenerForTesting listener) {
        return new Options.Builder()
            .server(cluster.aByName())
            .sslContext(context)
            .connectionListener(listener)
            .errorListener(listener)
            .noRandomize()
            .reconnectWait(Duration.ofMillis(100))
            .connectionTimeout(Duration.ofSeconds(2));
    }

    private static void awaitDiscovered(Connection nc, String server) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (!nc.getServers().contains(server) && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(nc.getServers().contains(server), "discovered: " + nc.getServers());
    }

    private static void roundTrip(Connection nc) throws Exception {
        String subject = "scenario." + System.nanoTime();
        Subscription sub = nc.subscribe(subject);
        nc.publish(subject, new byte[]{1});
        nc.flush(Duration.ofSeconds(2));
        assertNotNull(sub.nextMessage(Duration.ofSeconds(2)), "round trip after the reconnect");
    }

    // true when one reported exception has every needle somewhere in its cause chain
    private static boolean reported(ListenerForTesting listener, String... needles) {
        for (Exception e : listener.getExceptions()) {
            StringBuilder chain = new StringBuilder();
            for (Throwable t = e; t != null; t = t.getCause()) {
                chain.append(t.getClass().getName()).append(": ").append(t.getMessage()).append(" | ");
            }
            boolean all = true;
            for (String needle : needles) {
                all &= chain.toString().contains(needle);
            }
            if (all) {
                return true;
            }
        }
        return false;
    }

    // the seed is gone, so the only attempts that reach a server are those on the discovered one; its failure is reported
    private static void assertNeverReconnectedAndClosed(Connection nc, ListenerForTesting listener, String... failure) {
        assertTrue(listener.waitForStatusChange(20, TimeUnit.SECONDS), "closed once every server ran out");
        assertEquals(Connection.Status.CLOSED, nc.getStatus());
        assertEquals(0, listener.getEventCount(Events.RECONNECTED), "never reconnected");
        assertTrue(reported(listener, failure), "the discovered server's failure, " + Arrays.toString(failure) + ", in " + listener.getExceptions());
    }

    @Test
    public void s02_discoveredIpIsReconnectedToOverTlsUnderTheSeedsName() throws Exception {
        // S02: TLS, ip connect_urls, a certificate that names localhost only. The client is configured by name; the
        // discovered entry is an ip address, which inherits that name (ADR-40, the TLS name of a discovered server),
        // and the reconnect to it is over TLS, verified against the name, and carries traffic
        RecordingDataPort.protocols.clear();
        ListenerForTesting listener = new ListenerForTesting();
        SSLContext context = SslTestingHelper.createTestSSLContext();
        try (Cluster cluster = new Cluster(tls(CERT_NO_IP, KEY_NO_IP, false), tls(CERT_NO_IP, KEY_NO_IP, false))) {
            waitForClusterToForm(cluster, context);
            Options options = client(cluster, context, listener)
                .dataPortType(RecordingDataPort.class.getName())
                .maxReconnects(-1)
                .build();
            try (Connection nc = standardConnectionWait(Nats.connect(options))) {
                awaitDiscovered(nc, cluster.bDiscovered());
                listener.prepForStatusChange(Events.RECONNECTED);
                cluster.killA();
                assertTrue(listener.waitForStatusChange(10, TimeUnit.SECONDS), "reconnected to the discovered server");
                assertEquals(cluster.bDiscovered(), nc.getConnectedUrl());
                assertEquals(2, RecordingDataPort.protocols.size(), "one TLS session per server: " + RecordingDataPort.protocols);
                roundTrip(nc);
            }
        }
    }

    @Test
    public void s06_tlsIsKeptOnTheReconnectWhenTheServersOfferPlaintext() throws Exception {
        // S06: both servers have TLS but do not require it, allow_non_tls. A tls:// client upgrades on the seed and
        // again on the discovered server; the plaintext the servers offer is never taken (ADR-40, settings that ask
        // for TLS: the options decide, for every server)
        RecordingDataPort.protocols.clear();
        ListenerForTesting listener = new ListenerForTesting();
        SSLContext context = SslTestingHelper.createTestSSLContext();
        try (Cluster cluster = new Cluster(tls(CERT, KEY, true), tls(CERT, KEY, true))) {
            waitForClusterToForm(cluster, context);
            Options options = client(cluster, context, listener)
                .dataPortType(RecordingDataPort.class.getName())
                .maxReconnects(-1)
                .build();
            try (Connection nc = standardConnectionWait(Nats.connect(options))) {
                awaitDiscovered(nc, cluster.bDiscovered());
                listener.prepForStatusChange(Events.RECONNECTED);
                cluster.killA();
                assertTrue(listener.waitForStatusChange(10, TimeUnit.SECONDS), "reconnected to the discovered server");
                assertEquals(cluster.bDiscovered(), nc.getConnectedUrl());
                assertEquals(2, RecordingDataPort.protocols.size(), "one TLS session per server: " + RecordingDataPort.protocols);
                for (String protocol : RecordingDataPort.protocols) {
                    assertTrue(protocol.startsWith("TLSv1."), protocol);
                }
                roundTrip(nc);
            }
        }
    }

    @Test
    public void s16_discoveredServerWithACertificateFromAnUntrustedCaIsNotReconnectedTo() throws Exception {
        // S16: the discovered server presents a certificate from a CA the client does not trust. The reconnect to it
        // fails the handshake every time, the failure is reported, and the connection closes once every server has
        // run out of attempts, instead of connecting anywhere else. The JDK reports the trust failure as "PKIX path
        // validation failed" when the untrusted CA has the name of a trusted one, as every generated CA shares one
        // name, and as "PKIX path building failed" otherwise; the assertion accepts both
        Certificates trusted = new Certificates("localhost", true);
        Certificates untrusted = new Certificates("localhost", true); // its own CA
        PemFiles aPem = trusted.writePem();
        PemFiles bPem = untrusted.writePem();
        ListenerForTesting listener = new ListenerForTesting();
        SSLContext context = trusted.clientContext();
        try (Cluster cluster = new Cluster(tls(aPem.certFile, aPem.keyFile, false), tls(bPem.certFile, bPem.keyFile, false))) {
            waitForClusterToForm(cluster, context);
            Options options = client(cluster, context, listener).maxReconnects(2).build();
            Connection nc = standardConnectionWait(Nats.connect(options));
            awaitDiscovered(nc, cluster.bDiscovered());
            listener.prepForStatusChange(Events.CLOSED);
            cluster.killA();
            assertNeverReconnectedAndClosed(nc, listener, "PKIX path");
        }
    }

    @Test
    public void s17_discoveredServerWithATrustedCertificateForAnotherNameIsNotReconnectedTo() throws Exception {
        // S17: the discovered server presents a certificate the client trusts, for another name. The discovered ip
        // address inherited the seed's name, the check against it fails, the failure names the expected name and its
        // source, and the connection closes once every server has run out of attempts
        Certificates trusted = new Certificates("localhost", true);
        Certificates otherName = new Certificates("other.test", false, trusted);
        PemFiles aPem = trusted.writePem();
        PemFiles bPem = otherName.writePem();
        ListenerForTesting listener = new ListenerForTesting();
        SSLContext context = trusted.clientContext();
        try (Cluster cluster = new Cluster(tls(aPem.certFile, aPem.keyFile, false), tls(bPem.certFile, bPem.keyFile, false))) {
            waitForClusterToForm(cluster, context);
            Options options = client(cluster, context, listener).maxReconnects(2).build();
            Connection nc = standardConnectionWait(Nats.connect(options));
            awaitDiscovered(nc, cluster.bDiscovered());
            listener.prepForStatusChange(Events.CLOSED);
            cluster.killA();
            assertNeverReconnectedAndClosed(nc, listener, ":" + cluster.bPort, "localhost", SocketDataPort.TLS_HOST_FROM_REPORTING_SERVER);
        }
    }

    @Test
    public void s18_discoveredServerWithoutTlsIsNotReconnectedTo() throws Exception {
        // S18: a tls:// client behind a TLS seed; the discovered server has no TLS at all. The attempt on it fails
        // with the mismatch before anything is sent, the client never goes plaintext, and the connection closes once
        // every server has run out of attempts
        ListenerForTesting listener = new ListenerForTesting();
        SSLContext context = SslTestingHelper.createTestSSLContext();
        try (Cluster cluster = new Cluster(tls(CERT, KEY, false), plain())) {
            waitForClusterToForm(cluster, context);
            Options options = client(cluster, context, listener).maxReconnects(2).build();
            Connection nc = standardConnectionWait(Nats.connect(options));
            awaitDiscovered(nc, cluster.bDiscovered());
            listener.prepForStatusChange(Events.CLOSED);
            cluster.killA();
            assertNeverReconnectedAndClosed(nc, listener, "SSL connection wanted by client");
        }
    }

    private static Options.Builder fake(SniTestServer server, Certificates trusted, ListenerForTesting listener) throws Exception {
        return new Options.Builder()
            .server("tls://127.0.0.1:" + server.port())
            .sslContext(trusted.clientContext())
            .connectionListener(listener)
            .errorListener(listener)
            .connectionTimeout(Duration.ofSeconds(2));
    }

    private static void assertNothingInjectedWasActedOn(SniTestServer server, ListenerForTesting listener, String sentinel) throws Exception {
        assertEquals("", server.clientPlaintext.get(5, TimeUnit.SECONDS), "the client sent nothing in plaintext and closed");
        assertEquals(0, listener.getEventCount(Events.DISCOVERED_SERVERS));
        assertTrue(listener.getErrors().isEmpty(), "no server error was taken from the plaintext: " + listener.getErrors());
        for (Exception e : listener.getExceptions()) {
            assertFalse(String.valueOf(e.getMessage()).contains(sentinel), "the injected INFO was not applied: " + e);
        }
    }

    @Test
    public void s21_plaintextInjectedAfterInfoBeforeTheUpgradeIsNotActedOn() throws Exception {
        // S21: with TLS after INFO, the server's INFO is followed in the same write by plaintext that an on-path party
        // could have added: two PINGs and an INFO naming a sentinel server. The client must not answer the PINGs, over
        // TLS or in plaintext, and must not take the sentinel. It fails the attempt as a handshake failure that quotes
        // the first injected line (ADR-40, standard TLS)
        Certificates certificates = new Certificates(SniTestServer.HOST, true);
        ListenerForTesting listener = new ListenerForTesting();
        try (SniTestServer server = new SniTestServer(certificates, false, 0, true)) {
            server.plaintextAfterInfo = "PING\r\nPING\r\nINFO {\"server_id\":\"injected\",\"version\":\"2.12.0\",\"tls_required\":true,\"connect_urls\":[\"sentinel-injected.invalid:4222\"]}\r\n";
            Options options = fake(server, certificates, listener).maxReconnects(0).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
            assertNothingInjectedWasActedOn(server, listener, "sentinel-injected");
            assertTrue(reported(listener, "TLS handshake failed", "\"PING\""), listener.getExceptions().toString());
        }
    }

    @Test
    public void s26_plaintextOpBeforeInfoIsNotActedOn() throws Exception {
        // S26: with TLS after INFO, a protocol line arrives before INFO. The client must not act on it: the first
        // line it accepts is INFO. It fails the attempt and sends nothing, in plaintext or over TLS
        Certificates certificates = new Certificates(SniTestServer.HOST, true);
        ListenerForTesting listener = new ListenerForTesting();
        try (SniTestServer server = new SniTestServer(certificates, false, 0, true)) {
            server.plaintextBeforeInfo = "PING\r\n";
            Options options = fake(server, certificates, listener).maxReconnects(0).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
            assertNothingInjectedWasActedOn(server, listener, "sentinel-injected");
            assertTrue(reported(listener, "non-info"), listener.getExceptions().toString());
        }
    }

    @Test
    public void s25_infoOfAServerThatFailsVerificationDoesNotSteerTheClient() throws Exception {
        // S25: the seed presents a certificate the client does not trust; its plaintext INFO, read before the
        // handshake that then fails, advertises a canary whose certificate the client does trust. The client must not
        // take that INFO: the canary is never contacted and never enters the pool
        Certificates canaryCertificates = new Certificates(SniTestServer.HOST, true);
        Certificates seedCertificates = new Certificates(SniTestServer.HOST, true); // its own CA
        ListenerForTesting listener = new ListenerForTesting();
        try (SniTestServer canary = new SniTestServer(canaryCertificates, false, 0, true);
             SniTestServer seed = new SniTestServer(seedCertificates, false, Collections.singletonList("127.0.0.1:" + canary.port()), true)) {
            Options options = fake(seed, canaryCertificates, listener)
                // were the entry taken, this lets the client dial a discovered ip address behind an ip address seed
                .tlsVerifyDiscoveredByIp()
                .maxReconnects(2)
                .reconnectWait(Duration.ofMillis(50))
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
            assertTrue(reported(listener, "PKIX path"), listener.getExceptions().toString());
            Thread.sleep(500);
            assertEquals(0, canary.accepted.get(), "the canary was never contacted");
            assertEquals(0, listener.getEventCount(Events.DISCOVERED_SERVERS));
            for (Exception e : listener.getExceptions()) {
                assertFalse(String.valueOf(e.getMessage()).contains(":" + canary.port()), "the canary was never dialed: " + e);
            }
        }
    }

    @Test
    public void rootedDiscoveredNameIsDialedAsWrittenAndVerifiedWithoutTheDot() throws Exception {
        // a server advertised under a rooted name, localhost. with its trailing dot, is listed and dialed as written.
        // The name sent as SNI and verified against is localhost, without the dot: the certificate carries it that
        // way, and nats-server refuses a handshake whose server name ends in a dot (ADR-40, certificate verification).
        // The rule TlsHostnameTests covers against the fake server, here against two real nats-servers whose
        // certificate names localhost only
        RecordingDataPort.protocols.clear();
        RecordingDataPort.serverNames.clear();
        ListenerForTesting listener = new ListenerForTesting();
        SSLContext context = SslTestingHelper.createTestSSLContext();
        try (Cluster cluster = new Cluster(tls(CERT_NO_IP, KEY_NO_IP, false), tls(CERT_NO_IP, KEY_NO_IP, false), "localhost.")) {
            waitForClusterToForm(cluster, context);
            Options options = client(cluster, context, listener)
                .dataPortType(RecordingDataPort.class.getName())
                .maxReconnects(-1)
                .build();
            try (Connection nc = standardConnectionWait(Nats.connect(options))) {
                awaitDiscovered(nc, cluster.bDiscovered());
                listener.prepForStatusChange(Events.RECONNECTED);
                cluster.killA();
                assertTrue(listener.waitForStatusChange(10, TimeUnit.SECONDS), "reconnected to the discovered server");
                assertEquals(cluster.bDiscovered(), nc.getConnectedUrl(), "dialed as written, with the dot");
                assertEquals(Arrays.asList("localhost", "localhost"), RecordingDataPort.serverNames, "the server name sent to each server, without the dot");
                roundTrip(nc);
            }
        }
    }
}
