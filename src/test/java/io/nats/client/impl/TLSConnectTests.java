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
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.nats.client.impl.TestMacros.*;
import static org.junit.jupiter.api.Assertions.*;

public class TLSConnectTests {
    @Test
    public void testSimpleTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            SSLContext ctx = TestSSLUtils.createTestSSLContext();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testSimpleIPTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            SSLContext ctx = TestSSLUtils.createTestSSLContext();
            Options options = new Options.Builder().
                                server("127.0.0.1:" + ts.getPort()).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testVerifiedTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            SSLContext ctx = TestSSLUtils.createTestSSLContext();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testOpenTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                opentls().
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testURISchemeTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            Options options = new Options.Builder().
                                server("tls://localhost:"+ts.getPort()).
                                sslContext(TestSSLUtils.createTestSSLContext()). // override the custom one
                                maxReconnects(0).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testURISchemeIPTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            Options options = new Options.Builder().
                                server("tls://127.0.0.1:"+ts.getPort()).
                                sslContext(TestSSLUtils.createTestSSLContext()). // override the custom one
                                maxReconnects(0).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testURISchemeOpenTLSConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tls.conf", false)) {
            Options options = new Options.Builder().
                                server("opentls://localhost:"+ts.getPort()).
                                maxReconnects(0).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testTLSMessageFlow() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            SSLContext ctx = TestSSLUtils.createTestSSLContext();
            int msgCount = 100;
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);

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
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testTLSOnReconnect() throws InterruptedException, Exception {
        Connection nc = null;
        TestHandler handler = new TestHandler();
        int port = NatsTestServer.nextPort();
        int newPort = NatsTestServer.nextPort();

        // Use two server ports to avoid port release timing issues
        try {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", port, false)) {
                SSLContext ctx = TestSSLUtils.createTestSSLContext();
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    server(NatsTestServer.getURIForPort(newPort)).
                                    maxReconnects(-1).
                                    sslContext(ctx).
                                    connectionListener(handler).
                                    reconnectWait(Duration.ofMillis(10)).
                                    build();
                nc = Nats.connect(options);
                assertConnected(nc);
                assertTrue(((NatsConnection)nc).getDataPort() instanceof SocketDataPort, "Correct data port class");
                handler.prepForStatusChange(Events.DISCONNECTED);
            }

            ReconnectTests.flushAndWait(nc, handler);
            handler.prepForStatusChange(Events.RESUBSCRIBED);

            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", newPort, false)) {
                waitThenAssertConnected(nc, handler, 10000);
            }
        } finally {
            closeThenAssertClosed(nc);
        }
    }

    @Test
    public void testDisconnectOnUpgrade() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
                SSLContext ctx = TestSSLUtils.createTestSSLContext();
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(0).
                                    dataPortType(CloseOnUpgradeAttempt.class.getCanonicalName()).
                                    sslContext(ctx).
                                    build();
                try {
                    nc = Nats.connect(options);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testServerSecureClientNotMismatch() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(0).
                                    build();
                try {
                    nc = Nats.connect(options);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testClientSecureServerNotMismatch() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            try (NatsTestServer ts = new NatsTestServer()) {
                SSLContext ctx = TestSSLUtils.createTestSSLContext();
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(0).
                                    sslContext(ctx).
                                    build();
                try {
                    nc = Nats.connect(options);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testClientServerCertMismatch() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
                SSLContext ctx = TestSSLUtils.createEmptySSLContext();
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(0).
                                    sslContext(ctx).
                                    build();
                try {
                    nc = Nats.connect(options);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }
}