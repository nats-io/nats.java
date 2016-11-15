/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.Nats.ConnState.CLOSED;
import static io.nats.client.Nats.ConnState.CONNECTED;
import static io.nats.client.Nats.defaultOptions;
import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.newDefaultConnection;
import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static io.nats.client.UnitTestUtilities.runServerWithConfig;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.client.ConnectionImpl.Srv;
import org.hamcrest.core.IsNot;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Category(IntegrationTest.class)
public class ITConnectionTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(ITConnectionTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    ExecutorService executor = Executors.newFixedThreadPool(5);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testDefaultConnection() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection nc = newDefaultConnection()) {
                assertTrue(nc.isConnected());
            }
        }
    }

    @Test
    public void testConnectionStatus() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection nc = newDefaultConnection()) {
                assertEquals("Should have status set to CONNECTED", CONNECTED,
                        nc.getState());
                assertTrue("Should have status set to CONNECTED", nc.isConnected());
                nc.close();

                assertEquals("Should have status set to CLOSED", CLOSED, nc.getState());
                assertTrue("Should have status set to CLOSED", nc.isClosed());
            }
        }
    }

    @Test
    public void testConnClosedCb() throws Exception{
        final CountDownLatch cbLatch = new CountDownLatch(1);
        try (NatsServer srv = runDefaultServer()) {
            try (Connection nc = newDefaultConnection()) {
                nc.setClosedCallback(new ClosedCallback() {
                    @Override
                    public void onClose(ConnectionEvent event) {
                        cbLatch.countDown();
                    }
                });
                nc.close();
                assertTrue("Closed callback not triggered", cbLatch.await(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testCloseDisconnectedCb()
            throws Exception {
        final CountDownLatch cbLatch = new CountDownLatch(1);
        try (NatsServer srv = runDefaultServer()) {
            Thread.sleep(500);
            Options opts = new Options.Builder(Nats.defaultOptions())
                    .noReconnect().build();
            opts.url = Nats.DEFAULT_URL;
            opts.disconnectedCb = new DisconnectedCallback() {
                @Override
                public void onDisconnect(ConnectionEvent event) {
                    cbLatch.countDown();
                }
            };
            try (Connection nc = opts.connect()) {
                nc.close();
                assertTrue("Disconnected callback not triggered",
                        cbLatch.await(5, TimeUnit.SECONDS));
            }
        }
    }

    private Exception isRunningInAsyncCbDispatcher() {
        StackTraceElement[] stack =
                UnitTestUtilities.getStackTraceByName(Thread.currentThread().getName());
        for (StackTraceElement el : stack) {
            System.err.println(el);
            if (el.toString().contains("cbexec")) {
                return null;
            }
        }
        return new Exception(
                String.format("Callback not executed from dispatcher:\n %s\n", Arrays.toString
                        (stack)));
    }

    // @Test
    public void testCallbacksOrder() throws Exception {
        try (NatsServer authSrv = runServerWithConfig("src/test/resources/tls.conf")) {
            try (NatsServer srv = runDefaultServer()) {
                final AtomicBoolean firstDisconnect = new AtomicBoolean(true);
                final AtomicLong dtime1 = new AtomicLong();
                final AtomicLong dtime2 = new AtomicLong();
                final AtomicLong rtime = new AtomicLong();
                final AtomicLong atime1 = new AtomicLong();
                final AtomicLong atime2 = new AtomicLong();
                final AtomicLong ctime = new AtomicLong();

                final BlockingQueue<Throwable> cbErrors = new LinkedBlockingQueue<Throwable>(20);
                final CountDownLatch reconnected = new CountDownLatch(1);
                final CountDownLatch closed = new CountDownLatch(1);
                final CountDownLatch asyncErr = new CountDownLatch(2);
                final CountDownLatch recvLatch = new CountDownLatch(2);
                final CountDownLatch recvLatch1 = new CountDownLatch(1);
                final CountDownLatch recvLatch2 = new CountDownLatch(1);

                // fail("Not finished implementing");

                DisconnectedCallback dcb = new DisconnectedCallback() {
                    public void onDisconnect(ConnectionEvent event) {

                        Exception err = isRunningInAsyncCbDispatcher();
                        if (err != null) {
                            cbErrors.add(err);
                            return;
                        }
                        UnitTestUtilities.sleep(100);
                    }
                };
                // dcb.onDisconnect(null);
                Options opts = new Options.Builder().noReconnect().disconnectedCb(dcb).build();
                try (Connection nc = opts.connect()) {
                    srv.shutdown();
                    Thread.sleep(500);
                }
            }
            fail("Not finished implementing");
        }
    }

    @Test
    public void testServerStopDisconnectedCb() throws Exception{
        try (NatsServer srv = runDefaultServer()) {
            final CountDownLatch latch = new CountDownLatch(1);
            DisconnectedCallback dcb = new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    latch.countDown();
                }
            };
            Options opts = new Options.Builder(Nats.defaultOptions())
                    .noReconnect()
                    .disconnectedCb(dcb).build();
            try (Connection c = Nats.connect(Nats.DEFAULT_URL, opts)) {
                srv.shutdown();
                assertTrue("Disconnected callback not triggered", await(latch));
            }
        }
    }

    // TODO NOT IMPLEMENTED:
    // TestErrOnConnectAndDeadlock
    // TestClientCertificate
    // TestServerTLSHintConnections
    // TestErrOnConnectAndDeadlock
    // TestMoreErrOnConnect

    // @Test
    // public void testServerSecureConnections() throws Exception {
    // try (NatsServer srv = runServerWithConfig("tls.conf", true)) {
    // String secureUrl = "nats://derek:buckley@localhost:4443/";
    // ConnectionFactory cf = new ConnectionFactory(secureUrl);
    // try (Connection nc = cf.createConnection()) {
    // final byte[] omsg = "Hello World".getBytes();
    // final CountDownLatch latch = new CountDownLatch(1);
    // final AtomicInteger received = new AtomicInteger(0);
    //
    // try (Subscription sub = nc.subscribe("foo", new MessageHandler() {
    // public void onMessage(Message msg) {
    // received.incrementAndGet();
    // assertArrayEquals(omsg, msg.getData());
    // latch.countDown();
    // }
    // })) {
    // try {
    // nc.publish("foo", omsg);
    // } catch (Exception e) {
    // fail("Failed to publish on secure (TLS) connection");
    // }
    // nc.flush();
    // assertTrue("Didn't receive our message", latch.await(5, TimeUnit.SECONDS));
    // }
    // } catch (Exception e) {
    // e.printStackTrace();
    // fail("Failed to create secure (TLS) connection. " + e.getMessage());
    // }
    //
    // // Test flag mismatch
    // // Wanted but not available..
    // try (NatsServer ds = runDefaultServer()) {
    // ConnectionFactory cf2 = new ConnectionFactory();
    // cf2.setSecure(true);
    // try (Connection nc = cf2.createConnection()) {
    // fail("Should have failed to create a connection");
    // } catch (Exception e) {
    // assertTrue(e instanceof IOException);
    // assertEquals(ERR_SECURE_CONN_WANTED, e.getMessage());
    // }
    // }
    //
    // // TODO implement second half of this test from Go
    // }
    // }

    private static final String[] testServers = {"nats://localhost:1222", "nats://localhost:1223",
            "nats://localhost:1224", "nats://localhost:1225", "nats://localhost:1226",
            "nats://localhost:1227", "nats://localhost:1228"};

    @Test
    public void testServersRandomize() throws Exception{
        Options opts = new Options.Builder(defaultOptions()).build();
        opts.servers = Nats.processUrlArray(testServers);
        ConnectionImpl nc = new ConnectionImpl(opts);
        nc.setupServerPool();

        // build url string array from srvPool
        int idx = 0;
        String[] clientServers = new String[nc.getServerPool().size()];
        for (Srv s : nc.getServerPool()) {
            clientServers[idx++] = s.url.toString();
        }
        // In theory this could happen..
        assertThat("ServerPool list not randomized", clientServers, not(equalTo(testServers)));

//        nc.close();

        // Now test that we do not randomize if proper flag is set.
        opts = new Options.Builder(Nats.defaultOptions()).dontRandomize().build();
        opts.servers = Nats.processUrlArray(testServers);
        nc = new ConnectionImpl(opts);
        nc.setupServerPool();

        // build url string array from srvPool
        idx = 0;
        clientServers = new String[nc.getServerPool().size()];
        for (Srv s : nc.getServerPool()) {
            clientServers[idx++] = s.url.toString();
        }
        assertArrayEquals("ServerPool list should not be randomized", testServers, clientServers);

        /*
         * Although the original intent was that if Opts.Url is set, Opts.Servers is not (and vice
         * versa), the behavior is that Opts.Url is always first, even when randomization is
         * enabled. So make sure that this is still the case.
         */
        opts = Nats.defaultOptions();
        opts.servers = Nats.processUrlArray(testServers);
        opts.url = Nats.DEFAULT_URL;
        nc = new ConnectionImpl(opts);
        nc.setupServerPool();

        // build url string array from srvPool
        List<String> clientServerList = new ArrayList<String>();
        for (Srv s : nc.getServerPool()) {
            clientServerList.add(s.url.toString());
        }

        clientServers = clientServerList.toArray(new String[clientServerList.size()]);
        // In theory this could happen..
        assertThat("serverPool list not randomized", clientServers, IsNot.not(equalTo
                (testServers)));

        assertEquals(
                String.format("Options.url should be first in the array, got %s", clientServers[0]),
                Nats.DEFAULT_URL, clientServers[0]);
    }
}
