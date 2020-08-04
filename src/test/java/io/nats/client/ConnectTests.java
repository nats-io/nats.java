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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import io.nats.client.ConnectionListener.Events;
import io.nats.client.NatsServerProtocolMock.ExitAt;

public class ConnectTests {
    @Test
    public void testDefaultConnection() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(Options.DEFAULT_PORT, false)) {
            Connection nc = Nats.connect();
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testConnection() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testConnectionWithOptions() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).build();
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
    public void testFullFakeConnect() throws IOException, InterruptedException {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testFullFakeConnectWithTabs() throws IOException, InterruptedException {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            ts.useTabs();
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test(expected=IOException.class)
    public void testConnectExitBeforeInfo() throws IOException, InterruptedException {
        Connection nc = null;
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_BEFORE_INFO)) {
            Options opt = new Options.Builder().server(ts.getURI()).noReconnect().build();
            try {
                nc = Nats.connect(opt);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected=IOException.class)
    public void testConnectExitAfterInfo() throws IOException, InterruptedException {
        Connection nc = null;
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_INFO)) {
            Options opt = new Options.Builder().server(ts.getURI()).noReconnect().build();
            try {
                nc = Nats.connect(opt);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected=IOException.class)
    public void testConnectExitAfterConnect() throws IOException, InterruptedException {
        Connection nc = null;
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_CONNECT)) {
            Options opt = new Options.Builder().server(ts.getURI()).noReconnect().build();
            try {
                nc = Nats.connect(opt);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected=IOException.class)
    public void testConnectExitAfterPing() throws IOException, InterruptedException {
        Connection nc = null;
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_PING)) {
            Options opt = new Options.Builder().server(ts.getURI()).noReconnect().build();
            try {
                nc = Nats.connect(opt);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test
    public void testConnectionFailureWithFallback() throws IOException, InterruptedException {
        
        try (NatsTestServer ts = new NatsTestServer(false)) {
            try (NatsServerProtocolMock fake = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_PING)) {
                Options options = new Options.Builder().connectionTimeout(Duration.ofSeconds(5)).server(fake.getURI()).server(ts.getURI()).build();
                Connection nc = Nats.connect(options);
                try {
                    assertEquals("Connected Status", Connection.Status.CONNECTED, nc.getStatus());
                } finally {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test
    public void testConnectWithConfig() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/simple.conf", false)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testConnectWithCommas() throws IOException, InterruptedException {
        try (NatsTestServer ts1 = new NatsTestServer(false)) {
            try (NatsTestServer ts2 = new NatsTestServer(false)) {
                Connection nc = Nats.connect(ts1.getURI() + "," + ts2.getURI());
                try {
                    assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                } finally {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test
    public void testConnectRandomize() throws IOException, InterruptedException {
        try (NatsTestServer ts1 = new NatsTestServer(false)) {
            try (NatsTestServer ts2 = new NatsTestServer(false)) {
                int one = 0;
                int two = 0;

                // should get at least 1 for each
                for (int i=0; i < 10; i++) {
                    Connection nc = Nats.connect(ts1.getURI() + "," + ts2.getURI());
                    try {
                        assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

                        if (nc.getConnectedUrl().equals(ts1.getURI())) {
                            one++;
                        } else {
                            two++;
                        }
                    } finally {
                        nc.close();
                        assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                    }
                }

                assertTrue("randomly got one", one > 0);
                assertTrue("randomly got two", two > 0);
            }
        }
    }

    @Test
    public void testConnectNoRandomize() throws IOException, InterruptedException {
        try (NatsTestServer ts1 = new NatsTestServer(false)) {
            try (NatsTestServer ts2 = new NatsTestServer(false)) {
                int one = 0;
                int two = 0;

                // should get at least 1 for each
                for (int i=0; i < 10; i++) {
                    String[] servers = {ts1.getURI(), ts2.getURI()};
                    Options options = new Options.Builder().noRandomize().servers(servers).build();
                    Connection nc = Nats.connect(options);
                    try {
                        assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

                        if (nc.getConnectedUrl().equals(ts1.getURI())) {
                            one++;
                        } else {
                            two++;
                        }
                    } finally {
                        nc.close();
                        assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                    }
                }

                assertTrue("always got one", one == 10);
                assertTrue("never got two", two == 0);
            }
        }
    }

    @Test(expected=IOException.class)
    public void testFailWithMissingLineFeedAfterInfo() throws IOException, InterruptedException {
        Connection nc = null;
        String badInfo = "{\"server_id\":\"test\"}\rmore stuff";
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
            Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
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
    public void testFailWithStuffAfterInitialInfo() throws IOException, InterruptedException {
        Connection nc = null;
        String badInfo = "{\"server_id\":\"test\"}\r\nmore stuff";
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
            Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
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
    public void testFailWrongInitialInfoOP() throws IOException, InterruptedException {
        Connection nc = null;
        String badInfo = "PING {\"server_id\":\"test\"}\r\n"; // wrong op code
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
            ts.useCustomInfoAsFullInfo();
            Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
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
    public void testIncompleteInitialInfo() throws IOException, InterruptedException {
        Connection nc = null;
        String badInfo = "{\"server_id\"\r\n";
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
            Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
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
    
    @Test
    public void testAsyncConnection() throws IOException, InterruptedException {
        TestHandler handler = new TestHandler();
        Connection nc = null;

        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().
                                        server(ts.getURI()).
                                        connectionListener(handler).
                                        build();
           handler.prepForStatusChange(Events.CONNECTED);

            Nats.connectAsynchronously(options, false);

            handler.waitForStatusChange(1, TimeUnit.SECONDS);

            try {
                nc = handler.getConnection();
                assertNotNull(nc);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                if (nc != null) {
                    nc.close();
                }
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }
    
    @Test
    public void testAsyncConnectionWithReconnect() throws IOException, InterruptedException {
        TestHandler handler = new TestHandler();
        Connection nc = null;
        int port = NatsTestServer.nextPort();
        Options options = new Options.Builder().
                                server("nats://localhost:"+port).
                                maxReconnects(-1).
                                reconnectWait(Duration.ofMillis(100)).
                                connectionListener(handler).
                                build();
        
        try {
            Nats.connectAsynchronously(options, true);

            // No server at this point, let it fail and try to start over
            try {
                Thread.sleep(1000);
            } catch (Exception exp) {

            }

            nc = handler.getConnection(); // will be disconnected, but should be there
            assertNotNull(nc);

            handler.prepForStatusChange(Events.RECONNECTED);
            try (NatsTestServer ts = new NatsTestServer(port, false)) {
                handler.waitForStatusChange(5, TimeUnit.SECONDS);
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            }
        } finally {
            if (nc != null) {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnAsyncWithoutListener() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().
                                        server(ts.getURI()).
                                        build();
            Nats.connectAsynchronously(options, false);
        }
    }
    
    @Test
    public void testErrorOnAsync() throws IOException, InterruptedException {
        TestHandler handler = new TestHandler();
        Options options = new Options.Builder().
                                    server("nats://localhost:"+NatsTestServer.nextPort()).
                                    connectionListener(handler).
                                    errorListener(handler).
                                    noReconnect().
                                    build();
        handler.prepForStatusChange(Events.CLOSED);
        Nats.connectAsynchronously(options, false);
        handler.waitForStatusChange(10, TimeUnit.SECONDS);

        assertTrue(1 <= handler.getExceptionCount());
        assertTrue(handler.getConnection() == null || Connection.Status.CLOSED == handler.getConnection().getStatus());
    }

    @Test(expected=IOException.class)
    public void testConnectionTimeout() throws IOException, InterruptedException {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.SLEEP_BEFORE_INFO)) { // will sleep for 3
            Options options = new Options.Builder().
                                        server(ts.getURI()).
                                        noReconnect().
                                        traceConnection().
                                        connectionTimeout(Duration.ofSeconds(2)). // 2 is also the default but explicit for test
                                        build();
            Connection nc = Nats.connect(options);
            assertFalse("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
        }
    }

    @Test
    public void testSlowConnectionNoTimeout() throws IOException, InterruptedException {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.SLEEP_BEFORE_INFO)) {
            Options options = new Options.Builder().
                                        server(ts.getURI()).
                                        noReconnect().
                                        connectionTimeout(Duration.ofSeconds(6)). // longer than the sleep
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
    public void testTimeCheckCoverage() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(Options.DEFAULT_PORT, false)) {
            Options options = new Options.Builder().
                                        server(ts.getURI()).
                                        traceConnection().
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
    public void testConnectExceptionHasURLS() throws IOException, InterruptedException {
        try {
            Nats.connect("nats://testserver.notnats:4222, nats://testserver.alsonotnats:4223");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("testserver.notnats:4222"));
            assertTrue(e.getMessage().contains("testserver.alsonotnats:4223"));
        }
    }
}