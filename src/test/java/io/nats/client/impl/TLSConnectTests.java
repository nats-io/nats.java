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
import io.nats.client.support.Listener;
import io.nats.client.utils.CloseOnUpgradeAttempt;
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

import static io.nats.client.Options.PROP_SSL_CONTEXT_FACTORY_CLASS;
import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.OptionsUtils.optionsNoReconnect;
import static io.nats.client.utils.TestBase.*;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class TLSConnectTests {

    private static Options createTestOptionsManually(String... servers) throws Exception {
        return optionsBuilder(servers)
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
        return optionsBuilder(servers)
            .maxReconnects(0)
            .sslContextFactory(new SSLContextFactoryForTesting())
            .build();
    }

    private static Options createTestOptionsViaFactoryClassName(String... servers) {
        Properties properties = new Properties();
        properties.setProperty(PROP_SSL_CONTEXT_FACTORY_CLASS, SSLContextFactoryForTesting.class.getCanonicalName());
        return optionsBuilder(servers)
            .properties(properties)
            .maxReconnects(0)
            .sslContextFactory(new SSLContextFactoryForTesting())
            .build();
    }

    @Test
    public void testSimpleTLSConnection() throws Exception {
        runInSharedConfiguredServer("tls.conf", 1, ts1 -> {
            String servers = ts1.getServerUri();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        });
    }

    @Test
    public void testMultipleUrlTLSConnectionSetContext() throws Exception {
        runInSharedConfiguredServer("tls.conf", 1, ts1 ->
            runInSharedConfiguredServer("tls.conf", 2, ts2 -> {
                String[] servers = NatsTestServer.getLocalhostUris("tls", ts1, ts2);
                assertCanConnectAndPubSub(createTestOptionsManually(servers));
                assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
                assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
                assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
            }));
    }

    @Test
    public void testSimpleIPTLSConnection() throws Exception {
        runInSharedConfiguredServer("tls.conf", 1, ts1 -> {
            String servers = "127.0.0.1:" + ts1.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        });
    }

    @Test
    public void testURISchemeOpenTLSConnection() throws Exception {
        runInSharedConfiguredServer("tls.conf", 1, ts1 -> {
            String[] servers = NatsTestServer.getLocalhostUris("opentls", ts1);
            Options options = optionsBuilder(servers)
                .maxReconnects(0)
                .opentls()
                .build();
            assertCanConnectAndPubSub(options);

            Properties props = new Properties();
            props.setProperty(Options.PROP_SERVERS, String.join(",", servers));
            props.setProperty(Options.PROP_MAX_RECONNECT, "0");
            props.setProperty(Options.PROP_OPENTLS, "true");
            assertCanConnectAndPubSub(new Options.Builder(props).build());
        });
    }

    @Test
    public void testMultipleUrlOpenTLSConnection() throws Exception {
        runInSharedConfiguredServer("tls.conf", 1, ts1 ->
            runInSharedConfiguredServer("tls.conf", 2, ts2 -> {
                String[] servers = NatsTestServer.getLocalhostUris("opentls", ts1, ts2);
                Options options = optionsBuilder(servers)
                    .maxReconnects(0)
                    .opentls()
                    .build();
                assertCanConnectAndPubSub(options);

                Properties props = new Properties();
                props.setProperty(Options.PROP_SERVERS, String.join(",", servers));
                props.setProperty(Options.PROP_MAX_RECONNECT, "0");
                props.setProperty(Options.PROP_OPENTLS, "true");
                assertCanConnectAndPubSub(new Options.Builder(props).build());
            }));
    }

    @Test
    public void testProxyNotTlsFirst() throws Exception {
        runInSharedConfiguredServer("tls.conf", 1, ts1 -> {
            // 1. client regular secure | secure proxy | server insecure -> mismatch exception
            Listener listener = new Listener(true);
            ProxyConnection connRI = new ProxyConnection(ts1.getServerUri(), false, listener, SERVER_INSECURE);
            listener.queueException(IOException.class, "SSL connection wanted by client");
            assertThrows(Exception.class, () -> connRI.connect(false));
            sleep(100); // give time for listener to get message
            assertEquals(1, listener.getExceptionCount());

            // 2. client regular secure | secure proxy | server tls required  -> connects
            ProxyConnection connRR = new ProxyConnection(ts1.getServerUri(), false, null, SERVER_TLS_REQUIRED);
            connRR.connect(false);
            closeConnection(waitUntilConnected(connRR), 1000);

            // 3. client regular secure | secure proxy | server tls available -> connects
            ProxyConnection connRA = new ProxyConnection(ts1.getServerUri(), false, null, SERVER_TLS_AVAILABLE);
            connRA.connect(false);
            closeConnection(waitUntilConnected(connRA), 1000);
        });
    }

    @Test
    public void testOpenTLSConnection() throws Exception {
        runInSharedConfiguredServer("tls.conf", 1, ts1 -> {
            String servers = ts1.getServerUri();
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
        });
    }

    @Test
    public void testSimpleTlsFirstConnection() throws Exception {
        runInSharedConfiguredServer("tls_first.conf", ts -> {
            Options options = optionsBuilder(ts)
                .maxReconnects(0)
                .tlsFirst()
                .sslContext(SslTestingHelper.createTestSSLContext())
                .build();
            assertCanConnectAndPubSub(options);
        });
    }

    @Test
    public void testVerifiedTLSConnection() throws Exception {
        runInSharedConfiguredServer("tlsverify.conf", ts -> {
            String servers = ts.getServerUri();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        });
    }

    @Test
    public void testURISchemeTLSConnection() throws Exception {
        runInSharedConfiguredServer("tlsverify.conf", ts -> {
            String servers = "tls://localhost:" + ts.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        });
    }

    @Test
    public void testURISchemeIPTLSConnection() throws Exception {
        runInSharedConfiguredServer("tlsverify.conf", ts -> {
            String servers = "tls://127.0.0.1:" + ts.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryInstance(servers));
            assertCanConnectAndPubSub(createTestOptionsViaFactoryClassName(servers));
        });
    }

    @Test
    public void testTLSMessageFlow() throws Exception {
        runInSharedConfiguredServer("tlsverify.conf", ts -> {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            int msgCount = 100;
            Options options = optionsBuilder(ts)
                .maxReconnects(0)
                .sslContext(ctx)
                .build();
            try (Connection nc = standardConnect(options)) {
                Dispatcher d = nc.createDispatcher(
                    msg -> nc.publish(msg.getReplyTo(), new byte[16]));
                String subject = random();
                d.subscribe(subject);

                for (int i = 0; i < msgCount; i++) {
                    Future<Message> incoming = nc.request(subject, null);
                    Message msg = incoming.get(500, TimeUnit.MILLISECONDS);
                    assertNotNull(msg);
                    assertEquals(16, msg.getData().length);
                }
            }
        });
    }

    @Test
    public void testTLSOnReconnect() throws Exception {
        AtomicReference<NatsConnection> ncRef = new AtomicReference<>();
        Listener listener = new Listener(true);

        // Use two server ports to avoid port release timing issues
        int port = NatsTestServer.nextPort();
        int newPort = NatsTestServer.nextPort();

        runInConfiguredServer("tlsverify.conf", port, ts -> {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            Options options = optionsBuilder(ts.getServerUri(), NatsTestServer.getLocalhostUri(newPort))
                .maxReconnects(-1)
                .sslContext(ctx)
                .connectionListener(listener)
                .reconnectWait(Duration.ofMillis(10))
                .build();
            ncRef.set((NatsConnection)standardConnect(options));
            assertInstanceOf(SocketDataPort.class, ncRef.get().getDataPort(), "Correct data port class");
            listener.queueConnectionEvent(Events.DISCONNECTED);
        });

        NatsConnection nc = ncRef.get();
        flushConnection(nc);
        listener.validate();

        listener.queueConnectionEvent(Events.RESUBSCRIBED);
        runInConfiguredServer("tlsverify.conf", newPort, ts -> listener.validate());
        standardCloseConnection(nc);
    }

    @Test
    public void testDisconnectOnUpgrade() throws Exception {
        runInSharedConfiguredServer("tlsverify.conf", ts -> {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            Options options = optionsBuilder(ts)
                .maxReconnects(0)
                .dataPortType(CloseOnUpgradeAttempt.class.getCanonicalName())
                .sslContext(ctx)
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        });
    }

    @Test
    public void testServerSecureClientNotMismatch() throws Exception {
        runInSharedConfiguredServer("tlsverify.conf", ts -> {
            Options options = optionsNoReconnect(ts);
            assertThrows(IOException.class, () -> Nats.connect(options));
        });
    }

    @Test
    public void testClientSecureServerNotMismatch() throws Exception {
        runInSharedOwnNc(nc -> {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            Options options = optionsBuilder(nc).maxReconnects(0).sslContext(ctx).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        });
    }

    @Test
    public void testClientServerCertMismatch() throws Exception {
        Listener listener = new Listener();
        listener.queueException(CertificateException.class);
        runInSharedConfiguredServer("tlsverify.conf", ts -> {
            SSLContext ctx = SslTestingHelper.createEmptySSLContext();
            Options options = optionsBuilder(ts)
                .maxReconnects(0)
                .sslContext(ctx)
                .errorListener(listener)
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
            listener.validate();
        });
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
            Options.Builder builder = optionsBuilder(servers)
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
        // cannot check connect b/c tls first
        runInSharedConfiguredServer("tls_first.conf", ts -> {
            // 1. client tls first | secure proxy | server insecure -> connects
            ProxyConnection connTI = new ProxyConnection(ts.getServerUri(), true, null, SERVER_INSECURE);
            connTI.connect(false);
            closeConnection(waitUntilConnected(connTI), 1000);

            // 2. client tls first | secure proxy | server tls required -> connects
            ProxyConnection connTR = new ProxyConnection(ts.getServerUri(), true, null, SERVER_TLS_REQUIRED);
            connTR.connect(false);
            closeConnection(waitUntilConnected(connTR), 1000);

            // 3. client tls first | secure proxy | server tls available -> connects
            ProxyConnection connTA = new ProxyConnection(ts.getServerUri(), true, null, SERVER_TLS_AVAILABLE);
            connTA.connect(false);
            closeConnection(waitUntilConnected(connTA), 1000);
        });
    }
}