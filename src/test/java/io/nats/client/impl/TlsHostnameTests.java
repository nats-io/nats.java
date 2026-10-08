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
import io.nats.client.ConnectionListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.support.NatsUri;
import io.nats.client.support.ssl.DiagnosticSslContext;
import io.nats.client.support.ssl.SniTestServer;
import io.nats.client.support.ssl.TrustCheckEvent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.security.cert.CertificateExpiredException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduction: an SNI endpoint has a valid named certificate and an expired
 * default certificate. Losing the DNS name produces CertificateExpiredException
 * even though the intended certificate is valid.
 *
 * Run with: ./gradlew test --tests io.nats.client.impl.TlsHostnameTests
 */
public class TlsHostnameTests {
    private static SniTestServer.Certificates certificates;

    @BeforeAll
    public static void createCertificates() throws Exception {
        certificates = new SniTestServer.Certificates();
    }

    private Options.Builder options(SniTestServer server, DiagnosticSslContext context, boolean tlsFirst) {
        Options.Builder builder = new Options.Builder()
            .server("tls://" + SniTestServer.HOST + ":" + server.port())
            .serverPool(new NatsServerPool() {
                @Override
                public List<String> resolveHostToIps(String host, boolean maxOneResult, boolean includeIPV6) {
                    assertEquals(SniTestServer.HOST, host);
                    return Collections.singletonList("127.0.0.1");
                }
            })
            .sslContext(context)
            .noRandomize()
            .connectionTimeout(Duration.ofSeconds(2))
            .reconnectWait(Duration.ofMillis(10))
            .maxReconnects(2);
        if (tlsFirst) {
            builder.tlsFirst();
        }
        return builder;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void resolvedHostnameSelectsValidCertificate(boolean tlsFirst) throws Exception {
        DiagnosticSslContext context = certificates.clientContext();
        try (SniTestServer server = new SniTestServer(certificates, tlsFirst, 0);
             Connection nc = Nats.connect(options(server, context, tlsFirst).noReconnect().build())) {
            nc.flush(Duration.ofSeconds(2));
            assertEquals(SniTestServer.HOST, server.requestedName.get(2, TimeUnit.SECONDS));
            assertEquals(certificates.valid, server.selectedCertificate.get(2, TimeUnit.SECONDS));
            assertTrue(context.getTrustCheckEvents().stream().allMatch(e -> e.trusted));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void discoveredIpRetainsHostnameOnReconnect(boolean tlsFirst) throws Exception {
        DiagnosticSslContext context = certificates.clientContext();
        CountDownLatch reconnected = new CountDownLatch(1);
        try (SniTestServer second = new SniTestServer(certificates, tlsFirst, 0);
             SniTestServer first = new SniTestServer(certificates, tlsFirst, second.port());
             Connection nc = Nats.connect(options(first, context, tlsFirst)
                 .connectionListener((conn, event) -> {
                     if (event == ConnectionListener.Events.RECONNECTED) {
                         reconnected.countDown();
                     }
                 }).build())) {
            // The initial INFO supplies discovery before connectSucceeded.
            nc.flush(Duration.ofSeconds(2));
            assertEquals(SniTestServer.HOST, first.requestedName.get(2, TimeUnit.SECONDS));
            first.close();
            assertTrue(reconnected.await(10, TimeUnit.SECONDS), "Did not reconnect to discovered IP");
            nc.flush(Duration.ofSeconds(2));
            assertEquals("tls://127.0.0.1:" + second.port(), nc.getConnectedUrl());
            assertEquals(SniTestServer.HOST, second.requestedName.get(2, TimeUnit.SECONDS));
            assertEquals(certificates.valid, second.selectedCertificate.get(2, TimeUnit.SECONDS));
            assertEquals(2, context.getTrustCheckEvents().size());
            assertTrue(context.getTrustCheckEvents().stream().allMatch(e -> e.trusted));
        }
    }

    @Test
    public void explicitIpReproducesExpiredFallbackCertificate() throws Exception {
        DiagnosticSslContext context = certificates.clientContext();
        try (SniTestServer server = new SniTestServer(certificates, true, 0)) {
            Options options = new Options.Builder()
                .server("tls://127.0.0.1:" + server.port())
                .sslContext(context).tlsFirst().noReconnect()
                .connectionTimeout(Duration.ofSeconds(2)).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
            assertEquals("", server.requestedName.get(2, TimeUnit.SECONDS));
            assertEquals(certificates.expired, server.selectedCertificate.get(2, TimeUnit.SECONDS));
            assertEquals(1, context.getTrustCheckEvents().size());
            TrustCheckEvent event = context.getTrustCheckEvents().get(0);
            assertFalse(event.trusted);
            assertEquals(certificates.expired.getNotAfter(), event.chain[0].getNotAfter());
            Throwable cause = event.failure;
            while (cause != null && !(cause instanceof CertificateExpiredException)) {
                cause = cause.getCause();
            }
            assertNotNull(cause, "Expected CertificateExpiredException: " + event.failure);
        }
    }

    @Test
    public void tlsHostnameSurvivesResolutionWithoutChangingUriIdentity() throws Exception {
        NatsUri original = new NatsUri("tls://user:pass@nats.test:4222");
        for (String ip : Arrays.asList("127.0.0.1", "::1")) {
            NatsUri resolved = original.reHost(ip);
            assertEquals(SniTestServer.HOST, resolved.getTlsHost());
            assertEquals("user:pass", resolved.getUserInfo());
            assertTrue(resolved.hostIsIpAddress());
            NatsUri plain = new NatsUri(resolved.toString());
            assertEquals(plain, resolved);
            assertEquals(plain.hashCode(), resolved.hashCode());
            assertEquals(SniTestServer.HOST, resolved.reHost("127.0.0.2").getTlsHost());
            assertEquals("other.test", resolved.reHost("other.test").getTlsHost());
        }
        assertEquals("127.0.0.2", new NatsUri("tls://127.0.0.1:4222").reHost("127.0.0.2").getTlsHost());
    }

    @Test
    public void discoverySavesHostnameWithoutTlsSchemeAndHonorsIgnoreDiscoveredServers() {
        for (boolean ignore : new boolean[]{false, true}) {
            NatsServerPool pool = new NatsServerPool();
            Options.Builder builder = new Options.Builder().server("nats://one.test:4222");
            if (ignore) {
                builder.ignoreDiscoveredServers();
            }
            pool.initialize(builder.build());
            pool.nextServer();
            assertEquals(!ignore, pool.acceptDiscoveredUrls(Collections.singletonList("127.0.0.1:4222")));
            if (ignore) {
                assertEquals(1, pool.getServerList().size());
            }
            else {
                assertEquals("one.test", entry(pool, "127.0.0.1").getTlsHost());
            }
        }
    }

    @Test
    public void discoveryKeepsTlsNamesPerEntryAndDoesNotOverrideExplicitServers() throws Exception {
        NatsServerPool pool = new NatsServerPool();
        pool.initialize(new Options.Builder().noRandomize()
            .servers(new String[]{"tls://one.test:4222", "tls://two.test:4222", "tls://127.0.0.9:4222"}).build());
        assertEquals("one.test", pool.nextServer().getHost());
        List<String> discovered = Arrays.asList("127.0.0.1:4222", "[::1]:4222", "other.test:4222", "127.0.0.9:4222");
        pool.acceptDiscoveredUrls(discovered);
        assertEquals("one.test", entry(pool, "127.0.0.1").getTlsHost());
        assertEquals("one.test", entry(pool, "[::1]").getTlsHost());
        assertEquals("other.test", entry(pool, "other.test").getTlsHost());
        assertEquals("127.0.0.9", entry(pool, "127.0.0.9").getTlsHost());
        assertEquals("two.test", pool.nextServer().getHost());
        pool.acceptDiscoveredUrls(Arrays.asList("127.0.0.1:4222", "127.0.0.2:4222"));
        assertEquals("one.test", entry(pool, "127.0.0.1").getTlsHost());
        assertEquals("two.test", entry(pool, "127.0.0.2").getTlsHost());
        // Match Go: a connection whose URL is an IP does not supply a DNS name
        // for newly discovered servers.
        assertEquals("127.0.0.9", pool.nextServer().getHost());
        pool.acceptDiscoveredUrls(Collections.singletonList("127.0.0.3:4222"));
        assertEquals("127.0.0.3", entry(pool, "127.0.0.3").getTlsHost());
    }

    private NatsUri entry(NatsServerPool pool, String host) {
        return pool.entryList.stream().map(e -> e.nuri).filter(n -> n.getHost().equals(host))
            .findFirst().orElseThrow(() -> new AssertionError("Missing server " + host));
    }
}
