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

import io.nats.client.*;
import io.nats.client.ConnectionListener.Events;
import io.nats.client.utils.CloseOnUpgradeAttempt;
import io.nats.client.utils.OptionsUtils;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.NatsTestServer.configFileServer;
import static io.nats.client.NatsTestServer.skipConnectValidateServer;
import static io.nats.client.Options.PROP_SSL_CONTEXT_FACTORY_CLASS;
import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.TestBase.*;
import static io.nats.client.utils.ThreadUtils.sleep;
import static io.nats.client.utils.VersionUtils.atLeast2_10_3;
import static org.junit.jupiter.api.Assertions.*;

public class TLSConnectTests {

    private static Options createTestOptionsManually(String... servers) throws Exception {
        return OptionsUtils.optionsBuilder(servers)
            .maxReconnects(0)
            .sslContext(SslTestingHelper.createTestSSLContext())
            .build();
    }

    private static Options createTestOptionsViaProperties(String... servers) {
        Options options;
        Properties props = SslTestingHelper.createTestSSLProperties();
        props.setProperty(Options.PROP_SERVERS, String.join(",", servers));
        props.setProperty(Options.PROP_MAX_RECONNECT, "0");
        options = new Options.Builder(props).build();
        return options;
    }

    private static Options createTestOptionsViaFactoryInstance(String... servers) {
        return OptionsUtils.optionsBuilder(servers)
            .maxReconnects(0)
            .sslContextFactory(new SSLContextFactoryForTesting())
            .build();
    }

    private static Options createTestOptionsViaFactoryClassName(String... servers) {
        Properties properties = new Properties();
        properties.setProperty(PROP_SSL_CONTEXT_FACTORY_CLASS, SSLContextFactoryForTesting.class.getCanonicalName());
        return OptionsUtils.optionsBuilder(servers)
            .properties(properties)
            .maxReconnects(0)
            .sslContextFactory(new SSLContextFactoryForTesting())
            .build();
    }

    @Test
    public void testSimpleTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = configFileServer("tls.conf")) {
            String servers = ts.getServerUri();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testSimpleTlsFirstConnection() throws Exception {
        if (atLeast2_10_3(ensureVersionServerInfo())) {
            try (NatsTestServer ts = skipConnectValidateServer("tls_first.conf")) {
                Options options = OptionsUtils.optionsBuilder(ts)
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
        try (NatsTestServer ts = configFileServer("tls.conf")) {
            String[] servers = NatsTestServer.getLocalhostUris("tls", ts);
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testMultipleUrlTLSConnectionSetContext() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer server1 = configFileServer("tls.conf");
             NatsTestServer server2 = configFileServer("tls.conf")
        ) {
            String[] servers = NatsTestServer.getLocalhostUris("tls", server1, server2);
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testSimpleIPTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = configFileServer("tls.conf")) {
            String servers = "127.0.0.1:" + ts.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testVerifiedTLSConnection() throws Exception {
        try (NatsTestServer ts = configFileServer("tlsverify.conf")) {
            String servers = ts.getServerUri();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testOpenTLSConnection() throws Exception {
        try (NatsTestServer ts = configFileServer("tls.conf")) {
            String servers = ts.getServerUri();
            Options options = optionsBuilder()
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
        try (NatsTestServer ts = configFileServer("tlsverify.conf")) {
            String servers = "tls://localhost:"+ts.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testURISchemeIPTLSConnection() throws Exception {
        try (NatsTestServer ts = configFileServer("tlsverify.conf")) {
            String servers = "tls://127.0.0.1:"+ts.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        }
    }

    @Test
    public void testURISchemeOpenTLSConnection() throws Exception {
        try (NatsTestServer ts = configFileServer("tls.conf")) {
            String[] servers = NatsTestServer.getLocalhostUris("opentls", ts);
            Options options = OptionsUtils.optionsBuilder(servers)
                .maxReconnects(0)
                .opentls()
                .build();
            assertCanConnectAndPubSub(options);

            Properties props = new Properties();
            props.setProperty(Options.PROP_SERVERS, String.join(",", servers));
            props.setProperty(Options.PROP_MAX_RECONNECT, "0");
            props.setProperty(Options.PROP_OPENTLS, "true");
            assertCanConnectAndPubSub(new Options.Builder(props).build());
        }
    }

    @Test
    public void testMultipleUrlOpenTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer server1 = NatsTestServer.configFileServer("tls.conf");
             NatsTestServer server2 = NatsTestServer.configFileServer("tls.conf")
        ) {
            String[] servers = NatsTestServer.getLocalhostUris("opentls", server1, server2);
            Options options = OptionsUtils.optionsBuilder(servers)
                .maxReconnects(0)
                .opentls()
                .build();
            assertCanConnectAndPubSub(options);

            Properties props = new Properties();
            props.setProperty(Options.PROP_SERVERS, String.join(",", servers));
            props.setProperty(Options.PROP_MAX_RECONNECT, "0");
            props.setProperty(Options.PROP_OPENTLS, "true");
            assertCanConnectAndPubSub(new Options.Builder(props).build());
        }
    }

    @Test
    public void testTLSMessageFlow() throws Exception {
        try (NatsTestServer ts = NatsTestServer.configFileServer("tlsverify.conf")) {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            int msgCount = 100;
            Options options = OptionsUtils.optionsBuilder(ts)
                .maxReconnects(0)
                .sslContext(ctx)
                .build();
            Connection nc = standardConnectionWait(options);
            Dispatcher d = nc.createDispatcher(
                msg -> nc.publish(msg.getReplyTo(), new byte[16]));
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
    public void testTLSOnReconnect() throws Exception {
        Connection nc;
        ListenerForTesting listener = new ListenerForTesting();
        int port = NatsTestServer.nextPort();
        int newPort = NatsTestServer.nextPort();

        // Use two server ports to avoid port release timing issues
        try (NatsTestServer ts = configFileServer("tlsverify.conf", port)) {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            Options options = OptionsUtils.optionsBuilder(ts.getServerUri(), NatsTestServer.getLocalhostUri(newPort))
                .maxReconnects(-1)
                .sslContext(ctx)
                .connectionListener(listener)
                .reconnectWait(Duration.ofMillis(10))
                .build();
            nc = standardConnectionWait(options);
            assertInstanceOf(SocketDataPort.class, ((NatsConnection) nc).getDataPort(), "Correct data port class");
            listener.prepForStatusChange(Events.DISCONNECTED);
        }

        flushAndWaitLong(nc, listener);
        listener.prepForStatusChange(Events.RESUBSCRIBED);

        try (NatsTestServer ignored = configFileServer("tlsverify.conf", newPort)) {
            listenerConnectionWait(nc, listener, VERY_LONG_CONNECTION_WAIT_MS);
        }

        standardCloseConnection(nc);
    }

    @Test
    public void testDisconnectOnUpgrade() {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = NatsTestServer.configFileServer("tlsverify.conf")) {
                SSLContext ctx = SslTestingHelper.createTestSSLContext();
                Options options = OptionsUtils.optionsBuilder(ts)
                    .maxReconnects(0)
                    .dataPortType(CloseOnUpgradeAttempt.class.getCanonicalName())
                    .sslContext(ctx)
                    .build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testServerSecureClientNotMismatch() {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = NatsTestServer.configFileServer("tlsverify.conf")) {
                Options options = OptionsUtils.optionsBuilder(ts).maxReconnects(0).build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testClientSecureServerNotMismatch() {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer()) {
                SSLContext ctx = SslTestingHelper.createTestSSLContext();
                Options options = OptionsUtils.optionsBuilder(ts).maxReconnects(0).sslContext(ctx).build();
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
            try (NatsTestServer ts = NatsTestServer.configFileServer("tlsverify.conf")) {
                SSLContext ctx = SslTestingHelper.createEmptySSLContext();
                Options options = OptionsUtils.optionsBuilder(ts)
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
        optionsBuilder()
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
            Options.Builder builder = OptionsUtils.optionsBuilder(servers)
                .maxReconnects(0)
                .sslContext(SslTestingHelper.createTestSSLContext())
                .errorListener(listener);

            if (tlsFirst) {
                builder.tlsFirst();
            }

            return builder.build();
        }

        @Override
        void handleInfo(String infoJson) {
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
        if (atLeast2_10_3(ensureVersionServerInfo())) {
            // cannot check connect b/c tls first
            try (NatsTestServer ts = skipConnectValidateServer("tls_first.conf")) {
                // 1. client tls first | secure proxy | server insecure -> connects
                ProxyConnection connTI = new ProxyConnection(ts.getServerUri(), true, null, SERVER_INSECURE);
                connTI.connect(false);
                closeConnection(standardConnectionWait(connTI), 1000);

                // 2. client tls first | secure proxy | server tls required -> connects
                ProxyConnection connTR = new ProxyConnection(ts.getServerUri(), true, null, SERVER_TLS_REQUIRED);
                connTR.connect(false);
                closeConnection(standardConnectionWait(connTR), 1000);

                // 3. client tls first | secure proxy | server tls available -> connects
                ProxyConnection connTA = new ProxyConnection(ts.getServerUri(), true, null, SERVER_TLS_AVAILABLE);
                connTA.connect(false);
                closeConnection(standardConnectionWait(connTA), 1000);
            }
        }
    }

    @Test
    public void testProxyNotTlsFirst() throws Exception {
        try (NatsTestServer ts = NatsTestServer.configFileServer("tls.conf")) {
            // 1. client regular secure | secure proxy | server insecure -> mismatch exception
            ListenerForTesting listener = new ListenerForTesting();
            ProxyConnection connRI = new ProxyConnection(ts.getServerUri(), false, listener, SERVER_INSECURE);
            assertThrows(Exception.class, () -> connRI.connect(false));
            sleep(100); // give time for listener to get message
            assertEquals(1, listener.getExceptions().size());
            assertTrue(listener.getExceptions().get(0).getMessage().contains("SSL connection wanted by client"));

            // 2. client regular secure | secure proxy | server tls required  -> connects
            ProxyConnection connRR = new ProxyConnection(ts.getServerUri(), false, null, SERVER_TLS_REQUIRED);
            connRR.connect(false);
            closeConnection(standardConnectionWait(connRR), 1000);

            // 3. client regular secure | secure proxy | server tls available -> connects
            ProxyConnection connRA = new ProxyConnection(ts.getServerUri(), false, null, SERVER_TLS_AVAILABLE);
            connRA.connect(false);
            closeConnection(standardConnectionWait(connRA), 1000);
        }
    }
}