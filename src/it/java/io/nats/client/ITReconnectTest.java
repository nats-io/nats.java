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

import static io.nats.client.Nats.ConnState.CONNECTED;
import static io.nats.client.Nats.ConnState.RECONNECTING;
import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.runServerOnPort;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Category(IntegrationTest.class)
public class ITReconnectTest extends ITBaseTest {

    private final Properties reconnectOptions = getReconnectOptions();

    private static Properties getReconnectOptions() {
        Properties props = new Properties();

        props.setProperty(Nats.PROP_URL, "nats://localhost:22222");
        props.setProperty(Nats.PROP_RECONNECT_ALLOWED, Boolean.toString(true));
        props.setProperty(Nats.PROP_MAX_RECONNECT, Integer.toString(10));
        props.setProperty(Nats.PROP_RECONNECT_WAIT, Integer.toString(100));

        return props;
    }

    UnitTestUtilities utils = new UnitTestUtilities();

    @Test
    public void testReconnectDisallowedFlags() throws Exception {
        ConnectionFactory cf = new ConnectionFactory(reconnectOptions);
        cf.setUrl("nats://localhost:22222");
        cf.setReconnectAllowed(false);

        final CountDownLatch latch = new CountDownLatch(1);
        cf.setClosedCallback(new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
                latch.countDown();
            }
        });

        try (NatsServer ts = runServerOnPort(22222)) {
            try (Connection c = cf.createConnection()) {
                sleep(500);
                ts.shutdown();
                assertTrue(await(latch));
            }
        }
    }

    @Test
    public void testReconnectAllowedFlags() throws Exception {
        Options opts = new Options.Builder(Nats.defaultOptions())
                .maxReconnect(2)
                .reconnectWait(1, TimeUnit.SECONDS)
                .build();
        try (NatsServer ts = runServerOnPort(22222)) {
            final CountDownLatch ccLatch = new CountDownLatch(1);
            final CountDownLatch dcLatch = new CountDownLatch(1);
            try (final ConnectionImpl c = (ConnectionImpl) Nats.connect("nats://localhost:22222",
                    opts)) {
                c.setClosedCallback(new ClosedCallback() {
                    public void onClose(ConnectionEvent event) {
                        ccLatch.countDown();
                    }
                });

                c.setDisconnectedCallback(new DisconnectedCallback() {
                    public void onDisconnect(ConnectionEvent event) {
                        dcLatch.countDown();
                    }
                });

                ts.shutdown();

                // We want wait to timeout here, and the connection
                // should not trigger the Closed CB.
                assertFalse("ClosedCB should not be triggered yet", ccLatch.await(500, TimeUnit
                        .MILLISECONDS));


                // We should wait to get the disconnected callback to ensure
                // that we are in the process of reconnecting.
                assertTrue("DisconnectedCB should have been triggered", dcLatch.await(2, TimeUnit
                        .SECONDS));

                assertTrue("Expected to be in a reconnecting state", c.isReconnecting());
                c.setClosedCallback(null);
            }
        } // NatsServer
    }

    @Test
    public void testBasicReconnectFunctionality() throws Exception {
        ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
        cf.setMaxReconnect(10);
        cf.setReconnectWait(100);

        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch dcLatch = new CountDownLatch(1);

        cf.setDisconnectedCallback(new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
                dcLatch.countDown();
            }
        });
        final String testString = "bar";

        try (NatsServer ns1 = runServerOnPort(22222)) {
            try (Connection c = cf.createConnection()) {
                AsyncSubscription s = c.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {
                        String s = new String(msg.getData());
                        assertEquals("String doesn't match", testString, s);
                        latch.countDown();
                    }
                });

                c.flush();

                ns1.shutdown();
                // server is stopped here...

                assertTrue("Did not get the disconnected callback on time", await(dcLatch));

                UnitTestUtilities.sleep(50);
                c.publish("foo", testString.getBytes());

                // restart the server.
                try (NatsServer ns2 = runServerOnPort(22222)) {
                    c.setClosedCallback(null);
                    c.setDisconnectedCallback(null);
                    c.flush(5000);
                    assertTrue("Did not receive our message", await(latch));
                    assertEquals("Wrong number of reconnects.", 1, c.getStats().getReconnects());
                } // ns2
            } // Connection
        } // ns1
    }

    @Test
    public void testExtendedReconnectFunctionality() throws Exception {
        Properties opts = reconnectOptions;
        ConnectionFactory cf = new ConnectionFactory(opts);

        final CountDownLatch dcLatch = new CountDownLatch(1);
        cf.setDisconnectedCallback(new DisconnectedCallback() {
            public void onDisconnect(ConnectionEvent event) {
                dcLatch.countDown();
            }
        });

        final CountDownLatch rcLatch = new CountDownLatch(1);
        cf.setReconnectedCallback(new ReconnectedCallback() {
            public void onReconnect(ConnectionEvent event) {
                rcLatch.countDown();
            }
        });

        final AtomicInteger received = new AtomicInteger(0);

        final MessageHandler mh = new MessageHandler() {
            public void onMessage(Message msg) {
                received.incrementAndGet();
            }
        };

        byte[] payload = "bar".getBytes();
        try (NatsServer ns1 = runServerOnPort(22222)) {
            try (Connection c = cf.createConnection()) {
                c.subscribe("foo", mh);
                final Subscription foobarSub = c.subscribe("foobar", mh);

                c.publish("foo", payload);
                c.flush();

                ns1.shutdown();
                // server is stopped here.

                // wait for disconnect
                assertTrue("Did not receive a disconnect callback message",
                        await(dcLatch, 2, TimeUnit.SECONDS));

                // sub while disconnected.
                Subscription barSub = c.subscribe("bar", mh);

                // Unsub foobar while disconnected
                foobarSub.unsubscribe();

                c.publish("foo", payload);
                c.publish("bar", payload);

                try (NatsServer ns2 = runServerOnPort(22222)) {
                    // server is restarted here...
                    // wait for reconnect
                    assertTrue("Did not receive a reconnect callback message",
                            await(rcLatch, 2, TimeUnit.SECONDS));

                    c.publish("foobar", payload);
                    c.publish("foo", payload);

                    final CountDownLatch latch = new CountDownLatch(1);
                    c.subscribe("done", new MessageHandler() {
                        public void onMessage(Message msg) {
                            latch.countDown();
                        }
                    });
                    sleep(500, TimeUnit.MILLISECONDS);
                    c.publish("done", null);

                    assertTrue("Did not receive our 'done' message", await(latch));

                    // Sleep a bit to guarantee exec runs and processes all subs
                    sleep(50, TimeUnit.MILLISECONDS);

                    assertEquals(4, received.get());

                    c.setDisconnectedCallback(null);
                }
            }
        } // NatsServer ns
    }

    final Object mu = new Object();
    private final Map<Integer, Integer> results = new ConcurrentHashMap<Integer, Integer>();

    @Test
    public void testQueueSubsOnReconnect() throws Exception {
        ConnectionFactory cf = new ConnectionFactory(reconnectOptions);
        try (NatsServer ts = runServerOnPort(22222)) {

            final CountDownLatch latch = new CountDownLatch(1);
            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    latch.countDown();
                }
            });

            try (Connection c = cf.createConnection()) {
                assertFalse(c.isClosed());
                // TODO create encoded connection

                // Make sure we got what we needed, 1 msg only and all seqnos accounted for..
                MessageHandler cb = new MessageHandler() {
                    public void onMessage(Message msg) {
                        String num = new String(msg.getData());
                        Integer seqno = Integer.parseInt(num);
                        if (results.containsKey(seqno)) {
                            Integer val = results.get(seqno);
                            results.put(seqno, val++);
                        } else {
                            results.put(seqno, 1);
                        }
                    } // onMessage
                };


                String subj = "foo.bar";
                String qgroup = "workers";

                c.subscribe(subj, qgroup, cb);
                c.subscribe(subj, qgroup, cb);
                c.flush();

                // Base test
                sendAndCheckMsgs(c, subj, 10);

                // Stop and restart server
                ts.shutdown();

                // start back up
                try (NatsServer ts2 = runServerOnPort(22222)) {
                    sleep(500);
                    assertTrue("Did not fire ReconnectedCB", await(latch, 3, TimeUnit.SECONDS));
                    sendAndCheckMsgs(c, subj, 10);
                } // ts2
            } // connection
        } // ts

    }

    private void checkResults(int numSent) {
        for (int i = 0; i < numSent; i++) {
            if (results.containsKey(i)) {
                assertEquals("results count should have been 1 for " + String.valueOf(i), 1,
                        (int) results.get(i));
            } else {
                fail("results doesn't contain seqno: " + i);
            }
        }
        results.clear();
    }

    private void sendAndCheckMsgs(Connection conn, String subj, int numToSend) throws Exception {
        int numSent = 0;
        for (int i = 0; i < numToSend; i++) {
            conn.publish(subj, Integer.toString(i).getBytes());
            numSent++;
        }
        // Wait for processing
        conn.flush();
        sleep(50);

        checkResults(numSent);
    }

    @Test
    public void testIsClosed() throws Exception {
        ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
        cf.setReconnectAllowed(true);
        cf.setMaxReconnect(60);

        Connection c = null;
        try (NatsServer s1 = runServerOnPort(22222)) {
            c = cf.createConnection();
            assertFalse("isClosed returned true when the connection is still open.", c.isClosed());
        }

        assertFalse("isClosed returned true when the connection is still open.", c.isClosed());

        try (NatsServer s2 = runServerOnPort(22222)) {
            assertFalse("isClosed returned true when the connection is still open.", c.isClosed());

            c.close();
            assertTrue("isClosed returned false after close() was called.", c.isClosed());
        }
    }

    @Test
    public void testIsReconnectingAndStatus() throws Exception {

        try (NatsServer ts = runServerOnPort(22222)) {
            final CountDownLatch dcLatch = new CountDownLatch(1);
            final CountDownLatch rcLatch = new CountDownLatch(1);

            ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
            cf.setReconnectAllowed(true);
            cf.setMaxReconnect(10000);
            cf.setReconnectWait(100);

            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    dcLatch.countDown();
                }
            });

            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    rcLatch.countDown();
                }
            });

            // Connect, verify initial reconnecting state check, then stop the server
            try (Connection c = cf.createConnection()) {
                assertFalse("isReconnecting returned true when the connection is still open.",
                        c.isReconnecting());

                assertEquals(CONNECTED, c.getState());

                ts.shutdown();

                assertTrue("Disconnect callback wasn't triggered.", await(dcLatch));

                assertTrue("isReconnecting returned false when the client is reconnecting.",
                        c.isReconnecting());

                assertEquals(RECONNECTING, c.getState());

                // Wait until we get the reconnect callback
                try (NatsServer ts2 = runServerOnPort(22222)) {
                    assertTrue("reconnectedCb callback wasn't triggered.",
                            await(rcLatch, 3, TimeUnit.SECONDS));

                    assertFalse("isReconnecting returned true after the client was reconnected.",
                            c.isReconnecting());

                    assertEquals(CONNECTED, c.getState());

                    // Close the connection, reconnecting should still be false
                    c.close();

                    assertFalse("isReconnecting returned true after close() was called.",
                            c.isReconnecting());

                    assertTrue("Status returned " + c.getState()
                            + " after close() was called instead of CLOSED", c.isClosed());
                }
            }
        }
    }

    @Test
    public void testDefaultReconnectFailure() throws Exception {
        try (NatsServer ts = runServerOnPort(4222)) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setMaxReconnect(4);

            final CountDownLatch ccLatch = new CountDownLatch(1);

            try (Connection c = cf.createConnection()) {
                assertFalse(c.isClosed());

                final CountDownLatch dcLatch = new CountDownLatch(1);
                c.setDisconnectedCallback(new DisconnectedCallback() {
                    public void onDisconnect(ConnectionEvent event) {
                        dcLatch.countDown();
                    }
                });

                final CountDownLatch rcLatch = new CountDownLatch(1);
                c.setReconnectedCallback(new ReconnectedCallback() {
                    public void onReconnect(ConnectionEvent event) {
                        rcLatch.countDown();
                    }
                });

                c.setClosedCallback(new ClosedCallback() {
                    public void onClose(ConnectionEvent event) {
                        ccLatch.countDown();
                    }
                });

                ts.shutdown();

                assertTrue("Should have signaled disconnected",
                        await(dcLatch, 1, TimeUnit.SECONDS));

                assertFalse("Should not have reconnected", await(rcLatch, 1, TimeUnit.SECONDS));

            }

            assertTrue("Connection didn't close within timeout",
                    await(ccLatch, 2, TimeUnit.SECONDS));

            // assertEquals(0, c.getStats().getReconnects());
            // assertTrue("Should have been closed but is " + c.getState(), c.isClosed());

        } // ts
    }

    @Test
    public void testReconnectBufSize() throws Exception {
        try (NatsServer ts = runServerOnPort(4222)) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setReconnectBufSize(34); // 34 bytes

            final CountDownLatch dcLatch = new CountDownLatch(1);
            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent ev) {
                    dcLatch.countDown();
                }
            });

            try (ConnectionImpl nc = (ConnectionImpl) cf.createConnection()) {
                nc.flush();

                // Force disconnected state
                ts.shutdown();

                assertTrue("DisconnectedCB should have been triggered", await(dcLatch));

                byte[] msg = "food".getBytes(); // 4 bytes payload, total proto is 17 bytes

                // The buffer check is before the publish operation, here it will be 0
                nc.publish("foo", msg);

                // Here the buffer size will be 17 when we check
                nc.publish("foo", msg);

                // At this point, we are at the buffer limit
                assertEquals(34, nc.getPendingByteCount());

                // This should fail since we have exhausted the backing buffer.
                boolean exThrown = false;
                try {
                    nc.publish("foo", msg);
                } catch (IOException e) {
                    assertEquals(Nats.ERR_RECONNECT_BUF_EXCEEDED, e.getMessage());
                    exThrown = true;
                }
                assertTrue("Expected to fail to publish message: got no error", exThrown);

            }
        }
    }

    @Test
    public void testReconnectVerbose() throws Exception {
        try (NatsServer ts = runServerOnPort(4222)) {
            ConnectionFactory cf = new ConnectionFactory();
            cf.setVerbose(true);

            final CountDownLatch latch = new CountDownLatch(1);
            cf.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent ev) {
                    latch.countDown();
                }
            });

            try (Connection nc = cf.createConnection()) {
                nc.flush();

                ts.shutdown();
                sleep(500);
                try (NatsServer ts2 = runServerOnPort(4222)) {
                    assertTrue("Should have reconnected OK", await(latch));
                    nc.flush();
                }
            }
        }
    }

    // @Test
    // public void testFullFlushChanDuringReconnect() {
    // final ExecutorService exec = Executors.newSingleThreadExecutor();
    // try (NatsServer ts = utils.createServerOnPort(22222)) {
    // ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
    // cf.setReconnectAllowed(true);
    // cf.setMaxReconnect(10000);
    // cf.setReconnectWait(100);
    // final Channel<Boolean> rch = new Channel<Boolean>();
    // final Channel<Boolean> stop = new Channel<Boolean>();
    //
    // try (Connection nc = cf.createConnection()) {
    // // While connected, publish as fast as we can
    // exec.execute(new Runnable() {
    // public void run() {
    // for (int i=0; ; i++) {
    // try {
    // nc.publish("foo", "hello".getBytes());
    // // Make sure we are sending at least flushChanSize (1024) messages
    // // before potentially pausing.
    // if (i % 2000 == 0) {
    // try {
    // Boolean rv = stop.get(0);
    // if (rv != null) {
    // if (rv == true)
    // return;
    // } else {
    // sleep(100, TimeUnit.MILLISECONDS);
    // }
    // } catch (TimeoutException e) {
    // }
    // }
    // } catch (IOException e) {
    // }
    // }
    // }
    // });
    //
    // // Send a bit
    // sleep(500, TimeUnit.MILLISECONDS);
    //
    // // Shut down the server
    // ts.shutdown();
    //
    // // Continue sending wile we are disconnected
    // sleep(1, TimeUnit.SECONDS);
    //
    // stop.add(true);
    //
    // // Restart the server
    // try (NatsServer ts2 = utils.createServerOnPort(22222, true)) {
    // // Wait for the reconnect CB to be invoked (but not for too long)
    // assertTrue("Reconnect callback wasn't triggered", waitTime(rch, 5, TimeUnit.SECONDS));
    // }
    // exec.shutdownNow();
    // } catch (IOException | TimeoutException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // }
    // }
}
