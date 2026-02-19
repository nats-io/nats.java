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

import io.nats.client.*;
import io.nats.client.ConnectionListener.Events;
import io.nats.client.support.ssl.ExpiringClientCertUtil;
import io.nats.client.support.ssl.ExpiringComponents;
import io.nats.client.support.ssl.SslTestingHelper;
import io.nats.client.utils.CloseOnUpgradeAttempt;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.Options.PROP_SSL_CONTEXT_FACTORY_CLASS;
import static io.nats.client.utils.ResourceUtils.createTempDirectory;
import static io.nats.client.utils.ResourceUtils.deleteRecursive;
import static org.junit.jupiter.api.Assertions.*;

public class TLSConnectTests extends TestBase {

    private String convertToProtocol(String proto, NatsTestServer... servers) {
        StringBuilder sb = new StringBuilder();
        for (int x = 0; x < servers.length; x++) {
            if (x > 0) {
                sb.append(",");
            }
            sb.append(proto).append("://localhost:").append(servers[x].getPort());
        }
        return sb.toString();
    }

    private static Options createTestOptionsManually(String servers) throws Exception {
        return new Options.Builder()
            .server(servers)
            .maxReconnects(0)
            .sslContext(SslTestingHelper.createTestSSLContext())
            .build();
    }

    private static Options createTestOptionsViaProperties(String servers) {
        Options options;
        Properties props = SslTestingHelper.createTestSSLProperties();
        props.setProperty(Options.PROP_SERVERS, servers);
        props.setProperty(Options.PROP_MAX_RECONNECT, "0");
        options = new Options.Builder(props).build();
        return options;
    }

    private static Options createTestOptionsViaFactoryInstance(String servers) {
        return new Options.Builder()
            .server(servers)
            .maxReconnects(0)
            .sslContextFactory(new SSLContextFactoryForTesting())
            .build();
    }

    private static Options createTestOptionsViaFactoryClassName(String servers) {
        Properties properties = new Properties();
        properties.setProperty(PROP_SSL_CONTEXT_FACTORY_CLASS, SSLContextFactoryForTesting.class.getCanonicalName());
        return new Options.Builder(properties)
            .server(servers)
            .maxReconnects(0)
            .sslContextFactory(new SSLContextFactoryForTesting())
            .build();
    }

    @Test
    public void testSimpleTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            String servers = ts.getURI();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testSimpleTlsFirstConnection() throws Exception {
        if (TestBase.atLeast2_10_3(ensureRunServerInfo())) {
            try (NatsTestServer ts = new NatsTestServer(
                NatsTestServer.builder()
                    .configFilePath("src/test/resources/tls_first.conf")
                    .connectValidateTlsFirstMode())
            ) {
                String servers = ts.getURI();
                Options options = new Options.Builder()
                    .server(servers)
                    .maxReconnects(0)
                    .tlsFirst()
                    .sslContext(SslTestingHelper.createTestSSLContext())
                    .build();
                assertCanConnectAndPubSub(options);
            }
        }
    }

    @Test
    public void testSimpleUrlTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            String servers = convertToProtocol("tls", ts);
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testMultipleUrlTLSConnectionSetContext() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer server1 = new NatsTestServer("src/test/resources/tls.conf", false);
             NatsTestServer server2 = new NatsTestServer("src/test/resources/tls.conf", false);
        ) {
            String servers = convertToProtocol("tls", server1, server2);
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testSimpleIPTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            String servers = "127.0.0.1:" + ts.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testVerifiedTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            String servers = ts.getURI();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testOpenTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            String servers = ts.getURI();
            Options options = new Options.Builder()
                .server(servers)
                .maxReconnects(0)
                .opentls()
                .build();
            assertCanConnectAndPubSub(options);

            Properties props = new Properties();
            props.setProperty(Options.PROP_SERVERS, servers);
            props.setProperty(Options.PROP_MAX_RECONNECT, "0");
            props.setProperty(Options.PROP_OPENTLS, "true");
            assertCanConnectAndPubSub(new Options.Builder(props).build());
        }
    }

    @Test
    public void testURISchemeTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            String servers = "tls://localhost:"+ts.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testURISchemeIPTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            String servers = "tls://127.0.0.1:"+ts.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testURISchemeOpenTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            String servers = convertToProtocol("opentls", ts);
            Options options = new Options.Builder()
                .server(servers)
                .maxReconnects(0)
                .opentls()
                .build();
            assertCanConnectAndPubSub(options);

            Properties props = new Properties();
            props.setProperty(Options.PROP_SERVERS, servers);
            props.setProperty(Options.PROP_MAX_RECONNECT, "0");
            props.setProperty(Options.PROP_OPENTLS, "true");
            assertCanConnectAndPubSub(new Options.Builder(props).build());
        }
    }

    @Test
    public void testMultipleUrlOpenTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer server1 = new NatsTestServer("src/test/resources/tls.conf", false);
             NatsTestServer server2 = new NatsTestServer("src/test/resources/tls.conf", false);
        ) {
            String servers = convertToProtocol("opentls", server1, server2);
            Options options = new Options.Builder()
                .server(servers)
                .maxReconnects(0)
                .opentls()
                .build();
            assertCanConnectAndPubSub(options);

            Properties props = new Properties();
            props.setProperty(Options.PROP_SERVERS, servers);
            props.setProperty(Options.PROP_MAX_RECONNECT, "0");
            props.setProperty(Options.PROP_OPENTLS, "true");
            assertCanConnectAndPubSub(new Options.Builder(props).build());
        }
    }

    @Test
    public void testTLSMessageFlow() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            int msgCount = 100;
            Options options = new Options.Builder()
                .server(ts.getURI())
                .maxReconnects(0)
                .sslContext(ctx)
                .build();
            Connection nc = standardConnection(options);
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), new byte[16]);
            });
            d.subscribe("subject");

            for (int i=0;i<msgCount;i++) {
                Future<Message> incoming = nc.request("subject", null);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);
                assertNotNull(msg);
                assertEquals(16, msg.getData().length);
            }

            standardCloseConnection(nc);
        }
    }

    @Test
    public void testTLSOnReconnect() throws InterruptedException, Exception {
        Connection nc;
        ListenerForTesting listener = new ListenerForTesting();
        int port = NatsTestServer.nextPort();
        int newPort = NatsTestServer.nextPort();

        // Use two server ports to avoid port release timing issues
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", port, false)) {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    server(NatsTestServer.getNatsLocalhostUri(newPort)).
                    maxReconnects(-1).
                    sslContext(ctx).
                    connectionListener(listener).
                    reconnectWait(Duration.ofMillis(10)).
                    build();
            nc = standardConnection(options);
            assertInstanceOf(SocketDataPort.class, ((NatsConnection) nc).getDataPort(), "Correct data port class");
            listener.prepForStatusChange(Events.DISCONNECTED);
        }

        flushAndWaitLong(nc, listener);
        listener.prepForStatusChange(Events.RESUBSCRIBED);

        try (NatsTestServer ignored = new NatsTestServer("src/test/resources/tlsverify.conf", newPort, false)) {
            listenerConnectionWait(nc, listener, 10000);
        }

        standardCloseConnection(nc);
    }

    @Test
    public void testDisconnectOnUpgrade() {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
                SSLContext ctx = SslTestingHelper.createTestSSLContext();
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(0).
                                    dataPortType(CloseOnUpgradeAttempt.class.getCanonicalName()).
                                    sslContext(ctx).
                                    build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testServerSecureClientNotMismatch() {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(0).
                                    build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testClientSecureServerNotMismatch() {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer()) {
                SSLContext ctx = SslTestingHelper.createTestSSLContext();
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(0).
                                    sslContext(ctx).
                                    build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testClientServerCertMismatch() {
        AtomicReference<Exception> listenedException = new AtomicReference<>();
        ErrorListener el = new ErrorListener() {
            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                listenedException.set(exp);
            }
        };

        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
                SSLContext ctx = SslTestingHelper.createEmptySSLContext();
                Options options = new Options.Builder()
                        .server(ts.getURI())
                        .maxReconnects(0)
                        .sslContext(ctx)
                        .errorListener(el)
                        .build();
                Nats.connect(options);
            }
        });

        Exception e = listenedException.get();
        assertNotNull(e);
        assertInstanceOf(CertificateException.class, e.getCause().getCause());
    }

    @Test
    public void testSSLContextFactoryPropertiesPassOnCorrectly() throws NoSuchAlgorithmException {
        SSLContextFactoryForTesting factory = new SSLContextFactoryForTesting();
        new Options.Builder()
            .sslContextFactory(factory)
            .keystorePath("keystorePath")
            .keystorePassword("ksp".toCharArray())
            .truststorePath("truststorePath")
            .truststorePassword("tsp".toCharArray())
            .secure()
            .opentls()
            .tlsAlgorithm("tlsAlgorithm")
            .build();

        assertEquals("keystorePath", factory.properties.keystorePath);
        assertEquals("keystorePath", factory.properties.getKeystorePath());
        assertEquals("ksp", new String(factory.properties.keystorePassword));
        assertEquals("ksp", new String(factory.properties.getKeystorePassword()));
        assertEquals("truststorePath", factory.properties.truststorePath);
        assertEquals("truststorePath", factory.properties.getTruststorePath());
        assertEquals("tsp", new String(factory.properties.truststorePassword));
        assertEquals("tsp", new String(factory.properties.getTruststorePassword()));
        assertEquals("tlsAlgorithm", factory.properties.tlsAlgorithm);
        assertEquals("tlsAlgorithm", factory.properties.getTlsAlgorithm());
    }

    private static final int SERVER_INSECURE = 1;
    private static final int SERVER_TLS_AVAILABLE = 2;
    private static final int SERVER_TLS_REQUIRED = 3;
    static class ProxyConnection extends NatsConnection {
        int serverType;

        public ProxyConnection(String servers, boolean tlsFirst, ErrorListener listener, int serverType) throws Exception {
            super(makeMiddleman(servers, tlsFirst, listener));
            this.serverType = serverType;
        }

        private static Options makeMiddleman(String servers, boolean tlsFirst, ErrorListener listener) throws Exception {
            Options.Builder builder = new Options.Builder()
                .server(servers)
                .maxReconnects(0)
                .sslContext(SslTestingHelper.createTestSSLContext())
                .errorListener(listener);

            if (tlsFirst) {
                builder.tlsFirst();
            }

            return builder.build();
        }

        @Override
        protected void handleInfo(String infoJson) {
            switch (serverType) {
                case SERVER_INSECURE:
                    super.handleInfo(infoJson.replace(",\"tls_required\":true", "")); break;
                case SERVER_TLS_AVAILABLE:
                    super.handleInfo(infoJson.replace("\"tls_required\":true", "\"tls_available\":true")); break;
                default:
                    super.handleInfo(infoJson);
            }
        }
    }

    /*
        1. client tls first      | secure proxy | server insecure      -> connects
        2. client tls first      | secure proxy | server tls required  -> connects
        3. client tls first      | secure proxy | server tls available -> connects
        4. client regular secure | secure proxy | server insecure      -> mismatch exception
        5. client regular secure | secure proxy | server tls required  -> connects
        6. client regular secure | secure proxy | server tls available -> connects
    */
    @Test
    public void testProxyTlsFirst() throws Exception {
        if (TestBase.atLeast2_10_3(ensureRunServerInfo())) {
            // cannot check connect b/c tls first
            try (NatsTestServer ts = new NatsTestServer(
                NatsTestServer.builder()
                    .configFilePath("src/test/resources/tls_first.conf")
                    .connectValidateTlsFirstMode())
            ) {
                // 1. client tls first | secure proxy | server insecure -> connects
                ProxyConnection connTI = new ProxyConnection(ts.getURI(), true, null, SERVER_INSECURE);
                connTI.connect(false);
                closeConnection(standardConnectionWait(connTI), 1000);

                // 2. client tls first | secure proxy | server tls required -> connects
                ProxyConnection connTR = new ProxyConnection(ts.getURI(), true, null, SERVER_TLS_REQUIRED);
                connTR.connect(false);
                closeConnection(standardConnectionWait(connTR), 1000);

                // 3. client tls first | secure proxy | server tls available -> connects
                ProxyConnection connTA = new ProxyConnection(ts.getURI(), true, null, SERVER_TLS_AVAILABLE);
                connTA.connect(false);
                closeConnection(standardConnectionWait(connTA), 1000);
            }
        }
    }

    @Test
    public void testProxyNotTlsFirst() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            // 4. client regular secure | secure proxy | server insecure -> mismatch exception
            ListenerForTesting listener = new ListenerForTesting();
            ProxyConnection connRI = new ProxyConnection(ts.getURI(), false, listener, SERVER_INSECURE);
            assertThrows(Exception.class, () -> connRI.connect(false));
            assertEquals(1, listener.getExceptions().size());
            assertTrue(listener.getExceptions().get(0).getMessage().contains("SSL connection wanted by client"));

            // 5. client regular secure | secure proxy | server tls required  -> connects
            ProxyConnection connRR = new ProxyConnection(ts.getURI(), false, null, SERVER_TLS_REQUIRED);
            connRR.connect(false);
            closeConnection(standardConnectionWait(connRR), 1000);

            // 6. client regular secure | secure proxy | server tls available -> connects
            ProxyConnection connRA = new ProxyConnection(ts.getURI(), false, null, SERVER_TLS_AVAILABLE);
            connRA.connect(false);
            closeConnection(standardConnectionWait(connRA), 1000);
        }
    }

    @Test
    public void testConnectFailsFromSslContext() throws Exception {
        SSLContext sslContext = SslTestingHelper.getFailContext();

        SslTestErrorListener el = new SslTestErrorListener(1);

        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            Options options = new Options.Builder()
                .server(ts.getNatsLocalhostUri())
                .sslContext(sslContext)
                .maxReconnects(1)
                .connectionTimeout(Duration.ofSeconds(2))
                .errorListener(el)
                .build();

            try (Connection nc = Nats.connect(options)) {
                fail("Should not have connected");
            }
            catch (Exception e) {
                assertTrue(e.getMessage().contains("Unable to connect to NATS servers"));
            }

            assertTrue(el.latch.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testConnectFailsCertAlreadyExpired() throws Exception {
        Path tmpDir = null;
        try {
            ExpiringComponents expiring = ExpiringClientCertUtil.createExpired();
            validateExpiry(expiring, true);

            tmpDir = createTempDirectory();
            String configFilePath = expiring.writeNatsConfig(tmpDir);

            SslTestErrorListener el = new SslTestErrorListener(1);

            try (NatsTestServer ts = new NatsTestServer(configFilePath, false)) {
                Options options = new Options.Builder()
                    .server(ts.getNatsLocalhostUri())
                    .sslContext(expiring.sslContext)
                    .maxReconnects(1)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .errorListener(el)
                    .build();

                try (Connection nc = Nats.connect(options)) {
                    fail("should have thrown an exception");
                }
                catch (Exception e) {
                    assertTrue(e.getMessage().contains("Unable to connect to NATS servers"));
                }

                assertTrue(el.latch.await(2, TimeUnit.SECONDS));
            }
        }
        finally {
            deleteRecursive(tmpDir);
        }
    }

    static final int CLIENT_CERT_VALIDITY_MILLIS = 5000;

    @Test
    public void testReconnectFailsAfterCertExpires() throws Exception {
        Path tmpDir = null;
        try {
            ExpiringComponents expiring = ExpiringClientCertUtil.create(CLIENT_CERT_VALIDITY_MILLIS);
            validateExpiry(expiring, false);

            tmpDir = createTempDirectory();
            String configFilePath = expiring.writeNatsConfig(tmpDir);

            SslTestConnectionListener cl = new SslTestConnectionListener(1);
            SslTestErrorListener el = new SslTestErrorListener(2);

            Connection nc;
            try (NatsTestServer ts1 = new NatsTestServer(configFilePath, false)) {
                try (NatsTestServer ts2 = new NatsTestServer(configFilePath, false)) {
                    Options options = new Options.Builder()
                        .servers(new String[]{ts2.getNatsLocalhostUri(), ts1.getNatsLocalhostUri()})
                        .noRandomize()
                        .sslContext(expiring.sslContext)
                        .maxReconnects(1)
                        .connectionTimeout(Duration.ofSeconds(5))
                        .connectionListener(cl)
                        .errorListener(el)
                        .build();

                    nc = Nats.connect(options);
                    assertEquals(Connection.Status.CONNECTED, nc.getStatus());
                    assertTrue(cl.latch.await(2, TimeUnit.SECONDS));
                    sleep(CLIENT_CERT_VALIDITY_MILLIS); // sleep enough time for the cert to expire
                    validateExpiry(expiring, true);
                }
                assertTrue(el.latch.await(2, TimeUnit.SECONDS));
                nc.close();
            }
        }
        finally {
            deleteRecursive(tmpDir);
        }
    }

    @Test
    public void testForceReconnectFailsAfterCertExpires() throws Exception {
        Path tmpDir = null;
        try {
            ExpiringComponents expiring = ExpiringClientCertUtil.create(CLIENT_CERT_VALIDITY_MILLIS);
            validateExpiry(expiring, false);

            tmpDir = createTempDirectory();
            String configFilePath = expiring.writeNatsConfig(tmpDir);

            SslTestConnectionListener cl = new SslTestConnectionListener(1);
            SslTestErrorListener el = new SslTestErrorListener(2);

            try (NatsTestServer ts = new NatsTestServer(configFilePath, false)) {
                Options options = new Options.Builder()
                    .server(ts.getNatsLocalhostUri())
                    .sslContext(expiring.sslContext)
                    .maxReconnects(1)
                    .connectionTimeout(Duration.ofSeconds(5))
                    .connectionListener(cl)
                    .errorListener(el)
                    .build();

                try (Connection nc = Nats.connect(options)) {
                    assertEquals(Connection.Status.CONNECTED, nc.getStatus());
                    assertTrue(cl.latch.await(2, TimeUnit.SECONDS));
                    sleep(CLIENT_CERT_VALIDITY_MILLIS); // sleep enough time for the cert to expire
                    validateExpiry(expiring, true);
                    nc.forceReconnect();
                    assertTrue(el.latch.await(2, TimeUnit.SECONDS));
                }
            }
        }
        finally {
            deleteRecursive(tmpDir);
        }
    }

    private void validateExpiry(ExpiringComponents expiring, boolean expired) {
        Date expiry = expiring.sslContext.getClientCertificateExpiry();
        Date now = new Date();
        if (expired) {
            assertTrue(expiry.before(now));
        }
        else {
            assertFalse(expiry.before(now));
        }
    }

    static class SslTestConnectionListener implements ConnectionListener {
        public CountDownLatch latch;

        public SslTestConnectionListener(int latchAmount) {
            latch = new CountDownLatch(latchAmount);
        }

        @Override
        public void connectionEvent(Connection conn, Events type) {
            if (type == Events.CONNECTED) {
                latch.countDown();
            }
        }
    }

    static class SslTestErrorListener implements ErrorListener {
        public CountDownLatch latch;

        public SslTestErrorListener(int latchAmount) {
            latch = new CountDownLatch(latchAmount);
        }

        @Override
        public void exceptionOccurred(Connection conn, Exception exp) {
            if (hasSslOrSocketCauseInChain(exp)) {
                if (latch.getCount() > 0) {
                    latch.countDown();
                }
            }
        }

        boolean hasSslOrSocketCauseInChain(Throwable t) {
            while (t != null) {
                if (t instanceof SSLException || t instanceof SocketException) {
                    return true;
                }
                t = t.getCause();
            }
            return false;
        }
    }
}