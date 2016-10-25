/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.newDefaultConnection;
import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.nats.client.Constants.ConnState;

import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testDefaultConnection() throws IOException, TimeoutException {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection nc = newDefaultConnection()) {
                /* NOOP */
            }
        }
    }

    @Test
    public void testConnectionStatus() throws IOException, TimeoutException {
        try (NatsServer srv = runDefaultServer()) {
            try (Connection nc = newDefaultConnection()) {
                assertEquals("Should have status set to CONNECTED", ConnState.CONNECTED,
                        nc.getState());
                assertTrue("Should have status set to CONNECTED", nc.isConnected());
                nc.close();

                assertEquals("Should have status set to CLOSED", ConnState.CLOSED, nc.getState());
                assertTrue("Should have status set to CLOSED", nc.isClosed());
            }
        }
    }

    @Test
    public void testConnClosedCb() throws IOException, TimeoutException, InterruptedException {
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
            throws IOException, TimeoutException, InterruptedException {
        final CountDownLatch cbLatch = new CountDownLatch(1);
        try (NatsServer srv = runDefaultServer()) {
            Thread.sleep(500);
            ConnectionFactory cf = new ConnectionFactory();
            cf.setReconnectAllowed(false);
            try (Connection nc = cf.createConnection()) {
                nc.setDisconnectedCallback(new DisconnectedCallback() {
                    @Override
                    public void onDisconnect(ConnectionEvent event) {
                        cbLatch.countDown();
                    }
                });
                nc.close();
                assertTrue("Disconnected callback not triggered",
                        cbLatch.await(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testServerStopDisconnectedCb() throws IOException, TimeoutException {
        try (NatsServer srv = runDefaultServer()) {
            final CountDownLatch latch = new CountDownLatch(1);
            final ConnectionFactory cf = new ConnectionFactory();
            cf.setReconnectAllowed(false);
            cf.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    latch.countDown();
                }
            });

            try (Connection c = cf.createConnection()) {
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

    static String[] testServers = { "nats://localhost:1222", "nats://localhost:1223",
            "nats://localhost:1224", "nats://localhost:1225", "nats://localhost:1226",
            "nats://localhost:1227", "nats://localhost:1228" };

    @Test
    public void testServersRandomize() throws IOException, TimeoutException {
        Options opts = new Options();
        opts.setServers(testServers);
        ConnectionImpl nc = new ConnectionImpl(opts);
        nc.setupServerPool();
        // build url string array from srvPool
        int idx = 0;
        String[] clientServers = new String[nc.getServerPool().size()];
        for (ConnectionImpl.Srv s : nc.getServerPool()) {
            clientServers[idx++] = s.url.toString();
        }
        // In theory this could happen..
        Assert.assertThat(clientServers, IsNot.not(IsEqual.equalTo(testServers)));

        nc.close();

        // Now test that we do not randomize if proper flag is set.
        opts = new Options();
        opts.setServers(testServers);
        opts.setNoRandomize(true);
        nc = new ConnectionImpl(opts);
        nc.setupServerPool();
        // build url string array from srvPool
        idx = 0;
        clientServers = new String[nc.getServerPool().size()];
        for (ConnectionImpl.Srv s : nc.getServerPool()) {
            clientServers[idx++] = s.url.toString();
        }
        assertArrayEquals(testServers, clientServers);
        nc.close();
    }

    @Test
    public void testUrlIsFirst() throws IOException, TimeoutException {
        /*
         * Although the original intent was that if Opts.Url is set, Opts.Servers is not (and vice
         * versa), the behavior is that Opts.Url is always first, even when randomization is
         * enabled. So make sure that this is still the case.
         */
        Options opts = new Options();
        opts.setUrl(ConnectionFactory.DEFAULT_URL);
        opts.setServers(testServers);
        ConnectionImpl nc = new ConnectionImpl(opts);
        nc.setupServerPool();
        // build url string array from srvPool
        List<String> clientServerList = new ArrayList<String>();
        for (ConnectionImpl.Srv s : nc.getServerPool()) {
            clientServerList.add(s.url.toString());
        }

        String[] clientServers = clientServerList.toArray(new String[clientServerList.size()]);
        // In theory this could happen..
        Assert.assertThat("serverPool list not randomized", clientServers,
                IsNot.not(IsEqual.equalTo(testServers)));

        assertEquals(
                String.format("Options.Url should be first in the array, got %s", clientServers[0]),
                ConnectionFactory.DEFAULT_URL, clientServers[0]);
        nc.close();
    }
}
