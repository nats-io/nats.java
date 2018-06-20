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

import java.time.Duration;
import java.util.ArrayList;
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
import io.nats.client.SSLUtils;
import io.nats.client.utils.CloseOnUpgradeAttempt;
import io.nats.client.utils.SmallBufferSocketChannelDataPort;

public class TLSConnectTests {
    @Test
    public void testSimpleTLSConnection() throws Exception {
        //System.setProperty("javax.net.debug", "all");
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            SSLContext ctx = SSLUtils.createTestSSLContext();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testURISchemeTLSConnection() throws Exception {
        SSLContext.setDefault(SSLUtils.createTestSSLContext());
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", true)) {
            Options options = new Options.Builder().
                                server("tls://localhost:"+ts.getPort()).
                                maxReconnects(0).
                                build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testTLSMessageFlow() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            SSLContext ctx = SSLUtils.createTestSSLContext();
            int msgCount = 100;
            ArrayList<Future<Message>> messages = new ArrayList<>();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), new byte[16]);
            });
            d.subscribe("subject");

            for (int i=0;i<msgCount;i++) {
                Future<Message> incoming = nc.request("subject", null);
                messages.add(incoming);
            }

            for (Future<Message> f : messages) {
                Message msg = f.get(500, TimeUnit.MILLISECONDS);
                assertNotNull(msg);
                assertEquals(16, msg.getData().length);
            }

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testTLSOnReconnect() throws InterruptedException, Exception {
        Connection nc = null;

        try {
            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
                SSLContext ctx = SSLUtils.createTestSSLContext();
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(-1).
                                    sslContext(ctx).
                                    reconnectWait(Duration.ofMillis(10)).
                                    build();
                nc = Nats.connect(options);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            }

            try {
                Thread.sleep(200); // Could be flaky
                nc.flush(Duration.ofMillis(50)); //Trigger the reconnect
            } catch (Exception exp) {
                // exp.printStackTrace();
            }
            try {
                Thread.sleep(200); // Could be flaky
            } catch (Exception e) {
                e.printStackTrace();
            }

            assertTrue("Reconnecting status", Connection.Status.RECONNECTING == nc.getStatus());

            try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
                try {
                    Thread.sleep(200);
                } catch (Exception e) {
                }
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            }
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testTLSBufferResize() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            SSLContext ctx = SSLUtils.createTestSSLContext();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                dataPortType(SmallBufferSocketChannelDataPort.class.getCanonicalName()).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testDisconnectOnUpgrade() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            SSLContext ctx = SSLUtils.createTestSSLContext();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                dataPortType(CloseOnUpgradeAttempt.class.getCanonicalName()).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.DISCONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testServerSecureClientNotMismatch() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.DISCONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testClientSecureServerNotMismatch() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            SSLContext ctx = SSLUtils.createTestSSLContext();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.DISCONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testClientServerCertMismatch() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/tlsverify.conf", false)) {
            SSLContext ctx = SSLUtils.createEmptySSLContext();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                maxReconnects(0).
                                sslContext(ctx).
                                build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.DISCONNECTED == nc.getStatus());
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }
}