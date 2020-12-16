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

import io.nats.client.ConnectionListener.Events;
import io.nats.client.NatsServerProtocolMock.ExitAt;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.nats.client.impl.TestMacros.*;
import static org.junit.jupiter.api.Assertions.*;

public class ConnectTests {
    @Test
    public void testDefaultConnection() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(Options.DEFAULT_PORT, false)) {
            Connection nc = Nats.connect();
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testConnection() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testConnectionWithOptions() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testFullFakeConnect() throws IOException, InterruptedException {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testFullFakeConnectWithTabs() throws IOException, InterruptedException {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            ts.useTabs();
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testConnectExitBeforeInfo() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_BEFORE_INFO)) {
                Options opt = new Options.Builder().server(ts.getURI()).noReconnect().build();
                try {
                    nc = Nats.connect(opt);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testConnectExitAfterInfo() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_INFO)) {
                Options opt = new Options.Builder().server(ts.getURI()).noReconnect().build();
                try {
                    nc = Nats.connect(opt);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testConnectExitAfterConnect() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_CONNECT)) {
                Options opt = new Options.Builder().server(ts.getURI()).noReconnect().build();
                try {
                    nc = Nats.connect(opt);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testConnectExitAfterPing() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_PING)) {
                Options opt = new Options.Builder().server(ts.getURI()).noReconnect().build();
                try {
                    nc = Nats.connect(opt);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testConnectionFailureWithFallback() throws IOException, InterruptedException {

        try (NatsTestServer ts = new NatsTestServer(false)) {
            try (NatsServerProtocolMock fake = new NatsServerProtocolMock(ExitAt.EXIT_AFTER_PING)) {
                Options options = new Options.Builder().connectionTimeout(Duration.ofSeconds(5)).server(fake.getURI())
                        .server(ts.getURI()).build();
                Connection nc = Nats.connect(options);
                try {
                    assertConnected(nc);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        }
    }

    @Test
    public void testConnectWithConfig() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/simple.conf", false)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testConnectWithCommas() throws IOException, InterruptedException {
        try (NatsTestServer ts1 = new NatsTestServer(false)) {
            try (NatsTestServer ts2 = new NatsTestServer(false)) {
                Connection nc = Nats.connect(ts1.getURI() + "," + ts2.getURI());
                try {
                    assertConnected(nc);
                } finally {
                    closeThenAssertClosed(nc);
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
                for (int i = 0; i < 10; i++) {
                    Connection nc = Nats.connect(ts1.getURI() + "," + ts2.getURI());
                    try {
                        assertConnected(nc);

                        if (nc.getConnectedUrl().equals(ts1.getURI())) {
                            one++;
                        } else {
                            two++;
                        }
                    } finally {
                        closeThenAssertClosed(nc);
                    }
                }

                assertTrue(one > 0, "randomly got one");
                assertTrue(two > 0, "randomly got two");
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
                for (int i = 0; i < 10; i++) {
                    String[] servers = { ts1.getURI(), ts2.getURI() };
                    Options options = new Options.Builder().noRandomize().servers(servers).build();
                    Connection nc = Nats.connect(options);
                    try {
                        assertConnected(nc);

                        if (nc.getConnectedUrl().equals(ts1.getURI())) {
                            one++;
                        } else {
                            two++;
                        }
                    } finally {
                        closeThenAssertClosed(nc);
                    }
                }

                assertEquals(one, 10, "always got one");
                assertEquals(two, 0, "never got two");
            }
        }
    }

    @Test
    public void testFailWithMissingLineFeedAfterInfo() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            String badInfo = "{\"server_id\":\"test\"}\rmore stuff";
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
                Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
                try {
                    nc = Nats.connect(options);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testFailWithStuffAfterInitialInfo() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            String badInfo = "{\"server_id\":\"test\"}\r\nmore stuff";
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
                Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
                try {
                    nc = Nats.connect(options);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testFailWrongInitialInfoOP() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            String badInfo = "PING {\"server_id\":\"test\"}\r\n"; // wrong op code
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
                ts.useCustomInfoAsFullInfo();
                Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
                try {
                    nc = Nats.connect(options);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testIncompleteInitialInfo() {
        assertThrows(IOException.class, () -> {
            Connection nc = null;
            String badInfo = "{\"server_id\"\r\n";
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, badInfo)) {
                Options options = new Options.Builder().server(ts.getURI()).reconnectWait(Duration.ofDays(1)).build();
                try {
                    nc = Nats.connect(options);
                } finally {
                    closeThenAssertClosed(nc);
                }
            }
        });
    }

    @Test
    public void testAsyncConnection() throws IOException, InterruptedException {
        TestHandler handler = new TestHandler();
        Connection nc = null;

        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).connectionListener(handler).build();
            handler.prepForStatusChange(Events.CONNECTED);

            Nats.connectAsynchronously(options, false);

            handler.waitForStatusChange(1, TimeUnit.SECONDS);

            try {
                nc = handler.getConnection();
                assertNotNull(nc);
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testAsyncConnectionWithReconnect() throws IOException, InterruptedException {
        TestHandler handler = new TestHandler();
        Connection nc = null;
        int port = NatsTestServer.nextPort();
        Options options = new Options.Builder().server("nats://localhost:" + port).maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(100)).connectionListener(handler).build();

        try {
            Nats.connectAsynchronously(options, true);

            sleep(5000); // No server at this point, let it fail and try to start over

            nc = handler.getConnection(); // will be disconnected, but should be there
            assertNotNull(nc);

            handler.prepForStatusChange(Events.RECONNECTED);
            try (NatsTestServer ts = new NatsTestServer(port, false)) {
                waitThenAssertConnected(nc, handler);
            }
        } finally {
            closeThenAssertClosed(nc);
        }
    }

    @Test
    public void testThrowOnAsyncWithoutListener() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false)) {
                Options options = new Options.Builder().server(ts.getURI()).build();
                Nats.connectAsynchronously(options, false);
            }
        });
    }

    @Test
    public void testErrorOnAsync() throws IOException, InterruptedException {
        TestHandler handler = new TestHandler();
        Options options = new Options.Builder().server("nats://localhost:" + NatsTestServer.nextPort())
                .connectionListener(handler).errorListener(handler).noReconnect().build();
        handler.prepForStatusChange(Events.CLOSED);
        Nats.connectAsynchronously(options, false);
        handler.waitForStatusChange(10, TimeUnit.SECONDS);

        assertTrue(1 <= handler.getExceptionCount());
        assertTrue(handler.getConnection() == null || Connection.Status.CLOSED == handler.getConnection().getStatus());
    }

    @Test
    public void testConnectionTimeout() {
        assertThrows(IOException.class, () -> {
            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.SLEEP_BEFORE_INFO)) { // will sleep for 3
                Options options = new Options.Builder().server(ts.getURI()).noReconnect().traceConnection()
                        .connectionTimeout(Duration.ofSeconds(2)). // 2 is also the default but explicit for test
                build();
                Connection nc = Nats.connect(options);
                assertNotSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            }
        });
    }

    @Test
    public void testSlowConnectionNoTimeout() throws IOException, InterruptedException {
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.SLEEP_BEFORE_INFO)) {
            Options options = new Options.Builder().server(ts.getURI()).noReconnect()
                    .connectionTimeout(Duration.ofSeconds(6)). // longer than the sleep
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
    public void testTimeCheckCoverage() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(Options.DEFAULT_PORT, false)) {
            Options options = new Options.Builder().server(ts.getURI()).traceConnection().build();
            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }

    @Test
    public void testConnectExceptionHasURLS() {
        try {
            Nats.connect("nats://testserver.notnats:4222, nats://testserver.alsonotnats:4223");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("testserver.notnats:4222"));
            assertTrue(e.getMessage().contains("testserver.alsonotnats:4223"));
        }
    }

    @Test
    public void testFlushBuffer() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(Options.DEFAULT_PORT, false)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertConnected(nc);

                // test connected
                nc.flushBuffer();

                ts.shutdown();
                sleep(100);

                // test while reconnecting
                assertThrows(IllegalStateException.class, () -> nc.flushBuffer());
            } finally {
                closeThenAssertClosed(nc);
                // test when closed.
                assertThrows(IllegalStateException.class, () -> nc.flushBuffer());
            }
        }
    }

    @Test
    public void testFlushBufferThreadSafety() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(Options.DEFAULT_PORT, false)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertConnected(nc);

                // use two latches to sync the threads as close as
                // possible.
                CountDownLatch pubLatch = new CountDownLatch(1);
                CountDownLatch flushLatch = new CountDownLatch(1);
                CountDownLatch completedLatch = new CountDownLatch(1);

                Thread t = new Thread("publisher") {
                    public void run() {
                        byte[] payload = new byte[5];
                        pubLatch.countDown();
                        try {
                            flushLatch.await(2, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            // NOOP
                        }
                        for (int i = 1; i <= 50000; i++) {
                            nc.publish("foo", payload);
                            if (i % 2000 == 0) {
                                try {
                                    nc.flushBuffer();
                                } catch (IOException e) {
                                    break;
                                }
                            }
                        }
                        completedLatch.countDown();
                    }
                };
                
                t.start();

                // sync up the current thread and the publish thread
                // to get the most out of the test.
                try {
                   pubLatch.await(2, TimeUnit.SECONDS);
                } catch (Exception e) {
                    // NOOP
                }
                flushLatch.countDown();

                // flush as fast as we can while the publisher
                // is publishing.

                while (t.isAlive()) {
                    nc.flushBuffer();
                }

                // cleanup and doublecheck the thread is done.
                t.join(2000);

                // make sure the publisher actually completed.
                assertTrue(completedLatch.await(10, TimeUnit.SECONDS));
            } finally {
                closeThenAssertClosed(nc);
            }
        }
    }     
}