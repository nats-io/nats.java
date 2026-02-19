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
import io.nats.client.support.Listener;
import io.nats.client.support.ssl.SslTestingHelper;
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

import static io.nats.client.ConnectionListener.Events.CONNECTED;
import static io.nats.client.ConnectionListener.Events.RECONNECTED;
import static io.nats.client.NatsTestServer.configFileBuilder;
import static io.nats.client.NatsTestServer.nextPort;
import static io.nats.client.utils.ConnectionUtils.assertConnected;
import static io.nats.client.utils.ConnectionUtils.managedConnect;
import static io.nats.client.utils.OptionsUtils.NOOP_EL;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static org.junit.jupiter.api.Assertions.*;

public class WebsocketConnectTests extends TestBase {

    private static Options.Builder builder() {
        return Options.builder()
            .maxReconnects(0)
            .errorListener(NOOP_EL);
    }

    private static Options.Builder wsBuilder(NatsTestServer ts) {
        return builder()
            .server(NatsTestServer.getLocalhostUri(WS, ts.getPort(WS)));
    }

    private static Options.Builder wssBuilder(NatsTestServer ts) throws Exception {
        return builder()
            .server(NatsTestServer.getLocalhostUri(WSS, ts.getPort(WSS)))
            .sslContext(SslTestingHelper.createTestSSLContext());
    }

    private static void _test(Options.Builder builder) throws InterruptedException {
        try (Connection connection = managedConnect(builder.build())) {
            Dispatcher dispatcher = connection.createDispatcher(
                msg -> connection.publish(msg.getReplyTo(), (new String(msg.getData()) + ":reply").getBytes()));
            String subject = random();
            dispatcher.subscribe(subject);
            for (int x = 0; x < 10; x++) {
                String data = random() + x;
                Message response = connection.request(subject, data.getBytes()).join();
                assertEquals(data + ":reply", new String(response.getData()));
            }
        }
    }

    @Test
    public void testWs() throws Exception {
        runInSharedConfiguredServer("ws.conf", ts -> {
            _test(optionsBuilder(ts));
            _test(wsBuilder(ts));
        });
    }

    @Test
    public void testWss() throws Exception {
        runInSharedConfiguredServer("wss.conf", ts -> {
            _test(optionsBuilder(ts));
            _test(wssBuilder(ts));
        });
    }

    @Test
    public void testWssVerify() throws Exception {
        runInSharedConfiguredServer("wssverify.conf", ts -> {
            _test(optionsBuilder(ts));
            _test(wssBuilder(ts));
        });
    }

    private static java.util.function.Consumer<HttpRequest> getInterceptor() {
        return req -> {
            // Ideally we could validate that this header was sent to NATS server
            req.getHeaders().add("X-Ignored", "VALUE");
        };
    }

    @Test
    public void testWsInterceptor() throws Exception {
        runInSharedConfiguredServer("ws.conf",
            ts -> _test(wsBuilder(ts).httpRequestInterceptor(getInterceptor())));
    }

    @Test
    public void testWssInterceptor() throws Exception {
        runInSharedConfiguredServer("wss.conf",
            ts -> _test(wssBuilder(ts).httpRequestInterceptor(getInterceptor())));
    }

    @Test
    public void testWssVerifyInterceptor() throws Exception {
        runInSharedConfiguredServer("wssverify.conf",
            ts -> _test(wssBuilder(ts).httpRequestInterceptor(getInterceptor())));
    }

    @Test
    public void testWsOpenTLS() throws Exception {
        runInSharedConfiguredServer("ws.conf", ts -> _test(wsBuilder(ts).opentls()));
    }

    @Test
    public void testWssOpenTLS() throws Exception {
        runInSharedConfiguredServer("wss.conf", ts -> _test(wssBuilder(ts).opentls()));
    }

    @Test
    public void testWssVerifyOpenTLS() throws Exception {
        runInSharedConfiguredServer("wssverify.conf", ts -> _test(wssBuilder(ts).opentls()));
    }

    @Test
    public void testProxyRequestReply() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        RunProxy proxy = new RunProxy(new InetSocketAddress("localhost", 0), null, executor);
        executor.submit(proxy);

        runInSharedConfiguredServer("ws.conf", ts -> {
            Options.Builder builder = wsBuilder(ts)
                .proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", proxy.getPort())));
            _test(builder);
        });
    }

    @Test
    public void testWssTlsFirstIgnored() throws Exception {
        runInSharedConfiguredServer("wss.conf", ts -> _test(wssBuilder(ts).tlsFirst()));
    }

    @Test
    public void testWssVerifyTlsFirstIgnored() throws Exception {
        runInSharedConfiguredServer("wssverify.conf", ts -> _test(wssBuilder(ts).tlsFirst()));
    }

    @Test
    public void testTLSOnReconnect() throws Exception {
        Connection nc;
        Listener listener = new Listener();
        int port = nextPort();
        int wssPort = nextPort();

        // can't use shared b/c custom ports
        NatsServerRunner.Builder builder = configFileBuilder("wssverify.conf")
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

            listener.queueConnectionEvent(CONNECTED);
            nc = Nats.connect(options);
            assertInstanceOf(SocketDataPort.class, ((NatsConnection) nc).getDataPort(), "Correct data port class");
            listener.validate();
            assertConnected(nc);
        }

        listener.queueConnectionEvent(RECONNECTED);
        try (NatsTestServer ignored = new NatsTestServer(builder)) {
            listener.validate();
            assertConnected(nc);
        }
    }

    @Test
    public void testDisconnectOnUpgrade() throws Exception {
        runInSharedConfiguredServer("wssverify.conf", ts -> {
            SSLContext ctx = SslTestingHelper.createTestSSLContext();
            Options options = Options.builder()
                .server(ts.getLocalhostUri(WSS))
                .dataPortType(CloseOnUpgradeAttempt.class.getCanonicalName())
                .sslContext(ctx)
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        });
    }

    @Test
    public void testClientInsecureServerSecureMismatchWss() throws Exception {
        runInSharedConfiguredServer("wss.conf", ts -> {
            Options options = builder()
                .server(NatsTestServer.getLocalhostUri(WS, ts.getPort(WSS)))
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        });
    }

    @Test
    public void testClientInsecureServerSecureMismatchWssVerify() throws Exception {
        runInSharedConfiguredServer("wssverify.conf", ts -> {
            Options options = builder()
                .server(NatsTestServer.getLocalhostUri(WS, ts.getPort(WSS)))
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        });
    }

    @Test
    public void testClientSecureServerInsecureMismatch() throws Exception {
        runInSharedOwnNc(nc -> {
            //noinspection DataFlowIssue
            Options options = builder()
                .server(nc.getConnectedUrl())
                .sslContext(SslTestingHelper.createTestSSLContext())
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        });
    }

    @Test
    public void testClientServerCertMismatchWss() throws Exception {
        runInSharedConfiguredServer("wss.conf", ts -> {
            SSLContext ctx = SslTestingHelper.createEmptySSLContext();
            Options options = wssBuilder(ts).sslContext(ctx).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        });
    }

    @Test
    public void testClientServerCertMismatchWssVerify() throws Exception {
        runInSharedConfiguredServer("wssverify.conf", ts -> {
            SSLContext ctx = SslTestingHelper.createEmptySSLContext();
            Options options = wssBuilder(ts).sslContext(ctx).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        });
    }
}