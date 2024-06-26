// Copyright 2021 The NATS Authors
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

import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.nats.client.support.HttpRequest;
import io.nats.client.utils.CloseOnUpgradeAttempt;
import io.nats.client.utils.RunProxy;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.nats.client.ConnectionListener.Events.CONNECTED;
import static io.nats.client.NatsTestServer.*;
import static org.junit.jupiter.api.Assertions.*;

public class WebsocketConnectTests extends TestBase {

    @Test
    public void testRequestReply() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/ws.conf", false)) {
            standardRequestReply(Options.builder()
                .server(getNatsLocalhostUri(ts.getPort()))
                .maxReconnects(0).build());

            standardRequestReply(Options.builder().
                server(getLocalhostUri("ws", ts.getPort("ws")))
                .maxReconnects(0).build());
        }
    }

    private static void standardRequestReply(Options options) throws InterruptedException, IOException {
        try (Connection connection = standardConnection(options)) {
            Dispatcher dispatcher = connection.createDispatcher(msg -> {
                connection.publish(msg.getReplyTo(), (new String(msg.getData()) + ":REPLY").getBytes());
            });
            try {
                dispatcher.subscribe("TEST");
                Message response = connection.request("TEST", "REQUEST".getBytes()).join();
                assertEquals("REQUEST:REPLY", new String(response.getData()));
            } finally {
                dispatcher.drain(Duration.ZERO);
            }
        }
    }

    @Test
    public void testTLSRequestReply() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/wss.conf", false)) {

            java.util.function.Consumer<HttpRequest> interceptor = req -> {
                // Ideally we could validate that this header was sent to NATS server
                req.getHeaders().add("X-Ignored", "VALUE");
            };

            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            Options options = Options.builder()
                .httpRequestInterceptor(interceptor)
                .server(getLocalhostUri("wss", ts.getPort("wss")))
                .maxReconnects(0)
                .sslContext(ctx)
                .build();

            standardRequestReply(options);
        }
    }

    @Test
    public void testProxyRequestReply() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        RunProxy proxy = new RunProxy(new InetSocketAddress("localhost", 0), null, executor);
        executor.submit(proxy);

        try (NatsTestServer ts = new NatsTestServer("src/test/resources/ws.conf", false)) {
            Options options = Options.builder()
                .server(getLocalhostUri("ws", ts.getPort("ws")))
                .maxReconnects(0)
                .proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", proxy.getPort())))
                .build();
            standardRequestReply(options);
        }
    }

    @Test
    public void testSimpleTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/wss.conf", false)) {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            Options options = Options.builder()
                .server(getLocalhostUri("wss", ts.getPort("wss")))
                .maxReconnects(0)
                .sslContext(ctx)
                .build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testSimpleWSSIPConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/wss.conf", false)) {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            Options options = Options.builder().
                                server("wss://127.0.0.1:" + ts.getPort("wss")).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testVerifiedTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/wssverify.conf", false)) {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            Options options = Options.builder()
                .server(getLocalhostUri("wss", ts.getPort("wss")))
                .maxReconnects(0)
                .sslContext(ctx)
                .build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testOpenTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/wss.conf", false)) {
            Options options = Options.builder()
                .server(getLocalhostUri("wss", ts.getPort("wss")))
                .maxReconnects(0)
                .opentls()
                .build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testURIWSSHostConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/wssverify.conf", false)) {
            Options options = Options.builder()
                .server(getLocalhostUri("wss", ts.getPort("wss")))
                .sslContext(SslTestingHelper.createTestSSLContext())// override the custom one
                .maxReconnects(0)
                .build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testURIWSSIPConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/wssverify.conf", false)) {
            Options options = Options.builder()
                .server("wss://127.0.0.1:" + ts.getPort("wss"))
                .sslContext(SslTestingHelper.createTestSSLContext()) // override the custom one
                .maxReconnects(0)
                .build();
            assertCanConnect(options);
        }
    }

    @Test
    public void testURISchemeWSSConnection() throws Exception {
        SSLContext originalDefault = SSLContext.getDefault();
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/wss.conf", false)) {
            SSLContext.setDefault(SslTestingHelper.createTestSSLContext());
            Options options = Options.builder()
                .server(getLocalhostUri("wss", ts.getPort("wss")))
                .maxReconnects(0)
                .build();
            assertCanConnect(options);
        } finally {
            SSLContext.setDefault(originalDefault);
        }
    }

    @Test
    public void testURISchemeWSSConnectionEnsureTlsFirstHasNoEffect() throws Exception {
        SSLContext originalDefault = SSLContext.getDefault();
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/wss.conf", false)) {
            SSLContext.setDefault(SslTestingHelper.createTestSSLContext());
            Options options = Options.builder()
                .server(getLocalhostUri("wss", ts.getPort("wss")))
                .maxReconnects(0)
                .tlsFirst()
                .build();
            assertCanConnect(options);
        } finally {
            SSLContext.setDefault(originalDefault);
        }
    }

    @Test
    public void testTLSMessageFlow() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/wssverify.conf", false)) {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            int msgCount = 100;
            Options options = Options.builder()
                .server(getLocalhostUri("wss", ts.getPort("wss")))
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
    public void testTLSOnReconnect() throws Exception {
        Connection nc;
        ListenerForTesting listener = new ListenerForTesting();
        int port = nextPort();
        int wssPort = nextPort();

        NatsServerRunner.Builder builder = NatsServerRunner.builder().configFilePath("src/test/resources/wssverify.conf")
            .port(port)
            .port("wss", wssPort);
        SSLContext ctx = SslTestingHelper.createTestSSLContext();

        // Use two server ports to avoid port release timing issues
        try (NatsTestServer ignored = new NatsTestServer(builder)) {
            Options options = Options.builder()
                .server(getLocalhostUri("wss", wssPort))
                .noRandomize()
                .maxReconnects(-1)
                .sslContext(ctx)
                .connectionListener(listener)
                .reconnectWait(Duration.ofMillis(10))
                .build();

            listener.prepForStatusChange(CONNECTED);
            nc = Nats.connect(options);
            assertInstanceOf(SocketDataPort.class, ((NatsConnection) nc).getDataPort(), "Correct data port class");
            listener.waitForStatusChange(1, TimeUnit.SECONDS);
            assertEquals(Connection.Status.CONNECTED, nc.getStatus());
        }

        listener.prepForStatusChange(CONNECTED);
        try (NatsTestServer ignored = new NatsTestServer(builder)) {
            listener.waitForStatusChange(1, TimeUnit.SECONDS);
            assertEquals(Connection.Status.CONNECTED, nc.getStatus());
        }
    }

    @Test
    public void testDisconnectOnUpgrade() {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/wssverify.conf", false)) {
                SSLContext ctx = SslTestingHelper.createTestSSLContext();
                Options options = Options.builder()
                    .server(ts.getLocalhostUri("wss"))
                    .maxReconnects(0)
                    .dataPortType(CloseOnUpgradeAttempt.class.getCanonicalName())
                    .sslContext(ctx)
                    .build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testServerSecureClientNotMismatch() throws Exception {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/wssverify.conf", false)) {
                Options options = Options.builder()
                    .server(getLocalhostUri("ws", ts.getPort("wss")))
                    .maxReconnects(0)
                    .build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testClientSecureServerNotMismatch() {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer()) {
                SSLContext ctx = SslTestingHelper.createTestSSLContext();
                Options options = Options.builder()
                    .server(ts.getLocalhostUri("wss"))
                    .maxReconnects(0)
                    .sslContext(ctx)
                    .build();
                Nats.connect(options);
            }
        });
    }

    @Test
    public void testClientServerCertMismatch() {
        assertThrows(IOException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/wssverify.conf", false)) {
                SSLContext ctx = SslTestingHelper.createEmptySSLContext();
                Options options = Options.builder()
                    .server(ts.getLocalhostUri("wss"))
                    .maxReconnects(0)
                    .sslContext(ctx)
                    .build();
                Nats.connect(options);
            }
        });
    }
}