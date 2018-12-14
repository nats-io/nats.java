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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.TestHandler;
import io.nats.client.TestSSLUtils;
import io.nats.client.ConnectionListener.Events;
import io.nats.client.utils.CloseOnUpgradeAttempt;

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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertTrue("Correct data port class", ((NatsConnection)nc).getDataPort() instanceof SocketDataPort);
                handler.prepForStatusChange(Events.DISCONNECTED);
            }

            ReconnectTests.flushAndWait(nc, handler);
            handler.prepForStatusChange(Events.RESUBSCRIBED);

            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", newPort, false)) {
                handler.waitForStatusChange(10, TimeUnit.SECONDS);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            }
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test(expected=IOException.class)
    public void testDisconnectOnUpgrade() throws Exception {
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
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected=IOException.class)
    public void testServerSecureClientNotMismatch() throws Exception {
        Connection nc = null;
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                build();
            try {
                nc = Nats.connect(options);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected=IOException.class)
    public void testClientSecureServerNotMismatch() throws Exception {
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
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected=IOException.class)
    public void testClientServerCertMismatch() throws Exception {
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
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }
}