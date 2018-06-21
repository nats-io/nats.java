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

package io.nats.client;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.junit.Test;

import io.nats.client.ConnectionHandler.Events;

public class ConnectionHandlerTests {

    @Test
    public void testCloseCount() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            TestHandler handler = new TestHandler();
            Options options = new Options.Builder().
                                server(ts.getURI()).
                                connectionHandler(handler).
                                build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
            assertEquals(1, handler.getEventCount(Events.CLOSED));
        }
    }

    @Test
    public void testDiscoveredServersCountAndHandlerInOptions() throws Exception {

        try (NatsTestServer ts = new NatsTestServer()) {
            String customInfo = "{\"server_id\":\"myid\",\"connect_urls\": [\""+ts.getURI()+"\"]}";
            try (NatsServerProtocolMock ts2 = new NatsServerProtocolMock(null, customInfo)) {
                TestHandler handler = new TestHandler();
                Options options = new Options.Builder().
                                    server(ts2.getURI()).
                                    maxReconnects(0).
                                    connectionHandler(handler).
                                    build();
                Connection nc = Nats.connect(options);
                try {
                    assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                    assertEquals(1, handler.getEventCount(Events.DISCOVERED_SERVERS));
                } finally {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test
    public void testDisconnectReconnectCount() throws Exception {
        Connection nc = null;
        try {
            TestHandler handler = new TestHandler();
            int port;
            try (NatsTestServer ts = new NatsTestServer(false)) {
                Options options = new Options.Builder().
                                    server(ts.getURI()).
                                    reconnectWait(Duration.ofMillis(100)).
                                    connectionHandler(handler).
                                    build();
                port = ts.getPort();
                nc = Nats.connect(options);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            }

            try {
                Thread.sleep(100); // Could be flaky
            } catch (Exception e) {
                e.printStackTrace();
            }
            assertTrue(handler.getEventCount(Events.DISCONNECTED) > 1);


            try (NatsTestServer ts = new NatsTestServer(port, false)) {
                try {
                    Thread.sleep(200);
                } catch (Exception e) {
                }
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
                assertEquals(1, handler.getEventCount(Events.RECONNECTED));
            }
        } finally {
            nc.close();
        }
    }
}