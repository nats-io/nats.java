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
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public class TLSConnectTests {

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
            .sslContext(TestSSLUtils.createTestSSLContext())
            .build();
    }

    private static Options createTestOptionsViaProperties(String servers) {
        Options options;
        Properties props = TestSSLUtils.createTestSSLProperties();
        props.setProperty(Options.PROP_SERVERS, servers);
        props.setProperty(Options.PROP_MAX_RECONNECT, "0");
        options = new Options.Builder(props).build();
        return options;
    }

    @Test
    public void testSimpleTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            String servers = ts.getURI();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
        }
    }

    @Test
    public void testSimpleTlsFirstConnection() throws Exception {
        if (TestBase.atLeast2_10_3(ensureRunServerInfo())) {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls_first.conf", false)) {
                String servers = ts.getURI();
                Options options = new Options.Builder()
                    .server(servers)
                    .maxReconnects(0)
                    .tlsFirst()
                    .sslContext(TestSSLUtils.createTestSSLContext())
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
        }
    }

    @Test
    public void testSimpleIPTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            String servers = "127.0.0.1:" + ts.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
        }
    }

    @Test
    public void testVerifiedTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            String servers = ts.getURI();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
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
        }
    }

    @Test
    public void testURISchemeIPTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            String servers = "tls://127.0.0.1:"+ts.getPort();
            assertCanConnectAndPubSub(createTestOptionsManually(servers));
            assertCanConnectAndPubSub(createTestOptionsViaProperties(servers));
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
            SSLContext ctx = TestSSLUtils.createTestSSLContext();
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
        Connection nc = null;
        TestHandler handler = new TestHandler();
        int port = NatsTestServer.nextPort();
        int newPort = NatsTestServer.nextPort();

        // Use two server ports to avoid port release timing issues
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", port, false)) {
            SSLContext ctx = TestSSLUtils.createTestSSLContext();
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    server(NatsTestServer.getNatsLocalhostUri(newPort)).
                    maxReconnects(-1).
                    sslContext(ctx).
                    connectionListener(handler).
                    reconnectWait(Duration.ofMillis(10)).
                    build();
            nc = standardConnection(options);
            assertTrue(((NatsConnection)nc).getDataPort() instanceof SocketDataPort, "Correct data port class");
            handler.prepForStatusChange(Events.DISCONNECTED);
        }

        flushAndWaitLong(nc, handler);
        handler.prepForStatusChange(Events.RESUBSCRIBED);

        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", newPort, false)) {
            standardConnectionWait(nc, handler, 10000);
        }

        standardCloseConnection(nc);
    }

    @Test
    public void testDisconnectOnUpgrade() {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
                SSLContext ctx = TestSSLUtils.createTestSSLContext();
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
                SSLContext ctx = TestSSLUtils.createTestSSLContext();
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
            public void errorOccurred(Connection conn, String error) {}

            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                listenedException.set(exp);
            }

            @Override
            public void slowConsumerDetected(Connection conn, Consumer consumer) {}
        };

        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
                SSLContext ctx = TestSSLUtils.createEmptySSLContext();
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
        assertTrue(e.getCause().getCause() instanceof java.security.cert.CertificateException);
    }
}