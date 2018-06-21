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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.NatsServerProtocolMock;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;

public class ReconnectTests {
    @Test
    public void testSimpleReconnect() throws InterruptedException, IOException {
        Connection nc = null;
        int port;

        try {
            try (NatsTestServer ts = new NatsTestServer()) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(-1).
                                    reconnectWait(Duration.ofMillis(10)).
                                    build();
                                    port = ts.getPort();
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

            try (NatsTestServer ts = new NatsTestServer(port, false)) {
                try {
                    Thread.sleep(200);
                } catch (Exception e) {
                }
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            }

            assertEquals("reconnect count", 1, nc.getStatistics().getReconnects());
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testMaxReconnects() throws InterruptedException, IOException {
        Connection nc = null;

        try {
            try (NatsTestServer ts = new NatsTestServer()) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    maxReconnects(1).
                                    reconnectWait(Duration.ofMillis(10)).
                                    build();
                nc = Nats.connect(options);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            }

            try {
                Thread.sleep(100); // Could be flaky
                nc.flush(Duration.ofMillis(50)); //Trigger the reconnect
            } catch (Exception exp) {
                // exp.printStackTrace();
            }
            try {
                Thread.sleep(100); // Could be flaky
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testReconnectToSecondServer() throws InterruptedException, IOException {
        NatsConnection nc = null;

        try (NatsTestServer ts = new NatsTestServer()) {
            try (NatsTestServer ts2 = new NatsTestServer()) {
                Options options = new Options.Builder().server(ts2.getURI()).server(ts.getURI()).maxReconnects(-1).build();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(nc.getCurrentServerURI(), ts2.getURI());
            }

            try {
                Thread.sleep(200); // Could be flaky
                nc.flush(Duration.ofMillis(50)); //Trigger the reconnect
                Thread.sleep(200); // Could be flaky
            } catch (Exception e) {
            }

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getCurrentServerURI(), ts.getURI());
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testReconnectToSecondServerFromInfo() throws InterruptedException, IOException {
        NatsConnection nc = null;

        try (NatsTestServer ts = new NatsTestServer()) {
            String customInfo = "{\"server_id\":\"myid\",\"connect_urls\": [\""+ts.getURI()+"\"]}";
            try (NatsServerProtocolMock ts2 = new NatsServerProtocolMock(null, customInfo)) {
                Options options = new Options.Builder().server(ts2.getURI()).maxReconnects(-1).build();
                nc = (NatsConnection) Nats.connect(options);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals(nc.getCurrentServerURI(), ts2.getURI());
            }

            try {
                Thread.sleep(200); // Could be flaky
                nc.flush(Duration.ofMillis(50)); //Trigger the reconnect
                Thread.sleep(200); // Could be flaky
            } catch (Exception e) {
            }

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            assertEquals(nc.getCurrentServerURI(), ts.getURI());
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testOverflowReconnectBuffer() throws InterruptedException, IOException {
        Connection nc = null;
        try {
            try (NatsTestServer ts = new NatsTestServer()) {
                Options options = new Options.Builder().
                                        server(ts.getURI()).
                                        maxReconnects(-1).
                                        reconnectBufferSize(8*512).
                                        reconnectWait(Duration.ofSeconds(10)).
                                        build();
                nc = Nats.connect(options);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            }

            try {
                Thread.sleep(200); // Could be flaky
                nc.flush(Duration.ofMillis(50)); //Trigger the reconnect
                Thread.sleep(200); // Could be flaky
            } catch (Exception e) {
            }
            assertTrue("Reconnecting status", Connection.Status.RECONNECTING == nc.getStatus());

            for (int i=0;i<9;i++) {
                nc.publish("test", new byte[512]);// Should blow up by the 9th message
            }

            assertFalse(true);
        } finally {
            if (nc != null) {
                nc.close();
            }
        }
    }
}