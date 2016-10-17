/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Constants.ERR_NO_SERVERS;
import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.newDefaultConnection;
import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
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
        try (NATSServer srv = runDefaultServer()) {
            try (Connection nc = newDefaultConnection()) {
                /* NOOP */
            }
        }
    }

    @Test
    public void testConnectionStatus() throws IOException, TimeoutException {
        try (NATSServer srv = runDefaultServer()) {
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
        try (NATSServer srv = runDefaultServer()) {
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
        try (NATSServer srv = runDefaultServer()) {
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
        try (NATSServer srv = runDefaultServer()) {
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
    // try (NATSServer srv = runServerWithConfig("tls.conf", true)) {
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
    // try (NATSServer ds = runDefaultServer()) {
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

    @Test
    public void testConnectionTimeout() {
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        mcf.setThrowTimeoutException(true);
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
        } catch (IOException | TimeoutException e) {
            fail("Connection failed");
        }
    }

    static String[] testServers = { "nats://localhost:1222", "nats://localhost:1223",
            "nats://localhost:1224", "nats://localhost:1225", "nats://localhost:1226",
            "nats://localhost:1227", "nats://localhost:1228" };

    @Test
    public void testServersRandomize() throws IOException, TimeoutException {
        Options opts = new Options();
        opts.setServers(testServers);
        ConnectionImpl c = new ConnectionImpl(opts);
        c.setupServerPool();
        // build url string array from srvPool
        int i = 0;
        String[] clientServers = new String[c.getServerPool().size()];
        for (ConnectionImpl.Srv s : c.getServerPool()) {
            clientServers[i++] = s.url.toString();
        }
        // In theory this could happen..
        Assert.assertThat(clientServers, IsNot.not(IsEqual.equalTo(testServers)));

        // Now test that we do not randomize if proper flag is set.
        opts = new Options();
        opts.setServers(testServers);
        opts.setNoRandomize(true);
        c = new ConnectionImpl(opts);
        c.setupServerPool();
        // build url string array from srvPool
        i = 0;
        clientServers = new String[c.getServerPool().size()];
        for (ConnectionImpl.Srv s : c.getServerPool()) {
            clientServers[i++] = s.url.toString();
        }
        assertArrayEquals(testServers, clientServers);

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
        ConnectionImpl c = new ConnectionImpl(opts);
        c.setupServerPool();
        // build url string array from srvPool
        List<String> clientServerList = new ArrayList<String>();
        for (ConnectionImpl.Srv s : c.getServerPool()) {
            clientServerList.add(s.url.toString());
        }

        String[] clientServers = clientServerList.toArray(new String[clientServerList.size()]);
        // In theory this could happen..
        Assert.assertThat("serverPool list not randomized", clientServers,
                IsNot.not(IsEqual.equalTo(testServers)));

        assertEquals(
                String.format("Options.Url should be first in the array, got %s", clientServers[0]),
                ConnectionFactory.DEFAULT_URL, clientServers[0]);
    }

    @Test
    public void testExhaustedSrvPool() {
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            List<ConnectionImpl.Srv> pool = new ArrayList<ConnectionImpl.Srv>();
            c.setServerPool(pool);
            boolean exThrown = false;
            try {
                assertNull(c.currentServer());
                c.createConn();
            } catch (IOException e) {
                assertEquals(ERR_NO_SERVERS, e.getMessage());
                exThrown = true;
            }
            assertTrue("Should have thrown exception.", exThrown);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testReadLoopClosedConn() {
        // Tests to ensure that readLoop() breaks out if the connection is closed
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            c.close();
            BufferedInputStream br = mock(BufferedInputStream.class);
            c.setInputStream(br);
            assertEquals(br, c.getInputStream());
            doThrow(new IOException("readLoop() should already have terminated")).when(br)
                    .read(any(byte[].class), any(int.class), any(int.class));
            assertTrue(c.isClosed());
            c.readLoop();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testFlusherFalse() {
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            BufferedOutputStream bw = mock(BufferedOutputStream.class);
            doThrow(new IOException("Should not have flushed")).when(bw).flush();
            c.close();
            c.setOutputStream(bw);
            BlockingQueue<Boolean> fch = c.getFlushChannel();
            fch.add(false);
            c.setFlushChannel(fch);
            c.flusher();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testFlusherFlushError() {
        OutputStream bw = mock(OutputStream.class);
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            c.close();

        } catch (IOException | TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testNullTcpConnection() {
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
            c.setTcpConnection(null);
            assertEquals(null, c.getTcpConnection());
            c.close();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUnsubscribeAlreadyRemoved() {
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
            SyncSubscriptionImpl s = (SyncSubscriptionImpl) c.subscribeSync("foo");
            c.subs.remove(s.getSid());
            c.unsubscribe(s, 415);
            assertNotEquals(415, s.getMax());
        } catch (IOException | TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
