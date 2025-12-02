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
import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.NOOP_EL;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static org.junit.jupiter.api.Assertions.*;

public class WebsocketConnectTests extends TestBase {

    @Test
    public void testRequestReply() throws Exception {
        NatsTestServer ts = sharedConfigServer("ws.conf");
        standardRequestReply(Options.builder()
            .server(getLocalhostUri(ts.getPort()))
            .maxReconnects(0).build());

        standardRequestReply(Options.builder().
            server(NatsTestServer.getLocalhostUri(WS, ts.getPort(WS)))
            .maxReconnects(0).build());
    }

    private static void standardRequestReply(Options options) throws InterruptedException, IOException {
        try (Connection connection = standardConnect(options)) {
            Dispatcher dispatcher = connection.createDispatcher(
                msg -> connection.publish(msg.getReplyTo(), (new String(msg.getData()) + ":REPLY").getBytes()));
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
        NatsTestServer ts = sharedConfigServer("wss.conf");

        java.util.function.Consumer<HttpRequest> interceptor = req -> {
            // Ideally we could validate that this header was sent to NATS server
            req.getHeaders().add("X-Ignored", "VALUE");
        };

        SSLContext ctx = SslTestingHelper.createTestSSLContext();
        Options options = Options.builder()
            .httpRequestInterceptor(interceptor)
            .server(NatsTestServer.getLocalhostUri(WSS, ts.getPort(WSS)))
            .maxReconnects(0)
            .sslContext(ctx)
            .build();

        standardRequestReply(options);
    }

    @Test
    public void testProxyRequestReply() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        RunProxy proxy = new RunProxy(new InetSocketAddress("localhost", 0), null, executor);
        executor.submit(proxy);

        NatsTestServer ts = sharedConfigServer("ws.conf");
        Options options = Options.builder()
            .server(NatsTestServer.getLocalhostUri(WS, ts.getPort(WS)))
            .maxReconnects(0)
            .proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", proxy.getPort())))
            .errorListener(NOOP_EL)
            .build();
        standardRequestReply(options);
    }

    @Test
    public void testSimpleTLSConnection() throws Exception {
        NatsTestServer ts = sharedConfigServer("wss.conf");
        SSLContext ctx = SslTestingHelper.createTestSSLContext();
        Options options = Options.builder()
            .server(NatsTestServer.getLocalhostUri(WSS, ts.getPort(WSS)))
            .maxReconnects(0)
            .sslContext(ctx)
            .errorListener(NOOP_EL)
            .build();
        assertCanConnect(options);
    }

    @Test
    public void testSimpleWSSIPConnection() throws Exception {
        NatsTestServer ts = sharedConfigServer("wss.conf");
        SSLContext ctx = SslTestingHelper.createTestSSLContext();
        Options options = Options.builder()
            .server("wss://127.0.0.1:" + ts.getPort(WSS))
            .maxReconnects(0)
            .sslContext(ctx)
            .errorListener(NOOP_EL)
            .build();
        assertCanConnect(options);
    }

    @Test
    public void testVerifiedTLSConnection() throws Exception {
        NatsTestServer ts = sharedConfigServer( "wssverify.conf");
        SSLContext ctx = SslTestingHelper.createTestSSLContext();
        Options options = Options.builder()
            .server(NatsTestServer.getLocalhostUri(WSS, ts.getPort(WSS)))
            .maxReconnects(0)
            .sslContext(ctx)
            .errorListener(NOOP_EL)
            .build();
        assertCanConnect(options);
    }

    @Test
    public void testOpenTLSConnection() throws Exception {
        NatsTestServer ts = sharedConfigServer("wss.conf");
        Options options = Options.builder()
            .server(NatsTestServer.getLocalhostUri(WSS, ts.getPort(WSS)))
            .maxReconnects(0)
            .opentls()
            .build();
        assertCanConnect(options);
    }

    @Test
    public void testURIWSSHostConnection() throws Exception {
        SSLContext originalDefault = SSLContext.getDefault();
        try {
            NatsTestServer ts = sharedConfigServer( "wssverify.conf");
            Options options = Options.builder()
                .server(NatsTestServer.getLocalhostUri(WSS, ts.getPort(WSS)))
                .sslContext(SslTestingHelper.createTestSSLContext())// override the custom one
                .maxReconnects(0)
                .errorListener(NOOP_EL)
                .build();
            assertCanConnect(options);
        }
        finally {
            SSLContext.setDefault(originalDefault);
        }
    }

    @Test
    public void testURIWSSIPConnection() throws Exception {
        SSLContext originalDefault = SSLContext.getDefault();
        try {
            NatsTestServer ts = sharedConfigServer( "wssverify.conf");
            Options options = Options.builder()
                .server("wss://127.0.0.1:" + ts.getPort(WSS))
                .sslContext(SslTestingHelper.createTestSSLContext()) // override the custom one
                .maxReconnects(0)
                .build();
            assertCanConnect(options);
        }
        finally {
            SSLContext.setDefault(originalDefault);
        }
    }

    @Test
    public void testURISchemeWSSConnection() throws Exception {
        SSLContext originalDefault = SSLContext.getDefault();
        try {
            NatsTestServer ts = sharedConfigServer("wss.conf");
            SSLContext.setDefault(SslTestingHelper.createTestSSLContext());
            Options options = Options.builder()
                .server(NatsTestServer.getLocalhostUri(WSS, ts.getPort(WSS)))
                .maxReconnects(0)
                .build();
            assertCanConnect(options);
        }
        finally {
            SSLContext.setDefault(originalDefault);
        }
    }

    @Test
    public void testURISchemeWSSConnectionEnsureTlsFirstHasNoEffect() throws Exception {
        SSLContext originalDefault = SSLContext.getDefault();
        try {
            NatsTestServer ts = sharedConfigServer("wss.conf");
            SSLContext.setDefault(SslTestingHelper.createTestSSLContext());
            Options options = Options.builder()
                .server(NatsTestServer.getLocalhostUri(WSS, ts.getPort(WSS)))
                .maxReconnects(0)
                .tlsFirst()
                .errorListener(NOOP_EL)
                .build();
            assertCanConnect(options);
        }
        finally {
            SSLContext.setDefault(originalDefault);
        }
    }

    @Test
    public void testTLSMessageFlow() throws Exception {
        NatsTestServer ts = sharedConfigServer( "wssverify.conf");
        SSLContext ctx = SslTestingHelper.createTestSSLContext();
        int msgCount = 100;
        Options options = Options.builder()
            .server(NatsTestServer.getLocalhostUri(WSS, ts.getPort(WSS)))
            .maxReconnects(0)
            .sslContext(ctx)
            .build();
        try (Connection nc = standardConnect(options)) {
            Dispatcher d = nc.createDispatcher(msg -> nc.publish(msg.getReplyTo(), new byte[16]));
            String subject = random();
            d.subscribe(subject);

            for (int i = 0; i < msgCount; i++) {
                Future<Message> incoming = nc.request(subject, null);
                Message msg = incoming.get(500, TimeUnit.MILLISECONDS);
                assertNotNull(msg);
                assertEquals(16, msg.getData().length);
            }
        }
    }

    @Test
    public void testTLSOnReconnect() throws Exception {
        Connection nc;
        ListenerForTesting listener = new ListenerForTesting();
        int port = nextPort();
        int wssPort = nextPort();

        // can't use shared b/c custom ports
        NatsServerRunner.Builder builder = configFileBuilder( "wssverify.conf")
            .port(port)
            .port(WSS, wssPort);
        SSLContext ctx = SslTestingHelper.createTestSSLContext();

        // Use two server ports to avoid port release timing issues
        try (NatsTestServer ignored = new NatsTestServer(builder)) {
            Options options = Options.builder()
                .server(NatsTestServer.getLocalhostUri(WSS, wssPort))
                .noRandomize()
                .maxReconnects(-1)
                .sslContext(ctx)
                .connectionListener(listener)
                .errorListener(NOOP_EL)
                .reconnectWait(Duration.ofMillis(10))
                .build();

            listener.prepForStatusChange(CONNECTED);
            nc = Nats.connect(options);
            assertInstanceOf(SocketDataPort.class, ((NatsConnection) nc).getDataPort(), "Correct data port class");
            listener.waitForStatusChange(1, TimeUnit.SECONDS);
            assertConnected(nc);
        }

        listener.prepForStatusChange(CONNECTED);
        try (NatsTestServer ignored = new NatsTestServer(builder)) {
            listener.waitForStatusChange(1, TimeUnit.SECONDS);
            assertConnected(nc);
        }
    }

    @Test
    public void testDisconnectOnUpgrade() throws Exception {
        NatsTestServer ts = sharedConfigServer( "wssverify.conf");
        SSLContext ctx = SslTestingHelper.createTestSSLContext();
        Options options = Options.builder()
            .server(ts.getLocalhostUri(WSS))
            .maxReconnects(0)
            .dataPortType(CloseOnUpgradeAttempt.class.getCanonicalName())
            .sslContext(ctx)
            .errorListener(NOOP_EL)
            .build();
        assertThrows(IOException.class, () -> Nats.connect(options));
    }

    @Test
    public void testServerSecureClientNotMismatch() throws Exception {
        NatsTestServer ts = sharedConfigServer( "wssverify.conf");
        Options options = Options.builder()
            .server(NatsTestServer.getLocalhostUri(WS, ts.getPort(WSS)))
            .maxReconnects(0)
            .errorListener(NOOP_EL)
            .build();
        assertThrows(IOException.class, () -> Nats.connect(options));
    }

    @Test
    public void testClientSecureServerNotMismatch() throws Exception {
        runInSharedOwnNc(nc -> {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            //noinspection DataFlowIssue
            Options options = optionsBuilder()
                .server(nc.getConnectedUrl())
                .maxReconnects(0)
                .sslContext(ctx)
                .errorListener(NOOP_EL)
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        });
    }

    @Test
    public void testClientServerCertMismatch() throws Exception {
        NatsTestServer ts = sharedConfigServer( "wssverify.conf");
        SSLContext ctx = SslTestingHelper.createEmptySSLContext();
        Options options = Options.builder()
            .server(ts.getLocalhostUri(WSS))
            .maxReconnects(0)
            .sslContext(ctx)
            .errorListener(NOOP_EL)
            .build();
        assertThrows(IOException.class, () -> Nats.connect(options));
    }
}