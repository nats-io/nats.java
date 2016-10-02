/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.Constants.ERR_CONNECTION_CLOSED;
import static io.nats.client.Constants.ERR_NO_SERVERS;
import static io.nats.client.Constants.ERR_STALE_CONNECTION;
import static io.nats.client.UnitTestUtilities.bounceDefaultServer;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Category(IntegrationTest.class)
public class ITConnectionTest {
    final Logger logger = LoggerFactory.getLogger(ITConnectionTest.class);

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    ExecutorService executor = Executors.newFixedThreadPool(5);
    // UnitTestUtilities utils = new UnitTestUtilities();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        UnitTestUtilities.startDefaultServer();
        Thread.sleep(500);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        UnitTestUtilities.stopDefaultServer();
        Thread.sleep(500);
    }

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    // TODO finish this test
    // @Test
    // public void testDoReconnectIoErrors() {
    // String infoString =
    // "INFO
    // {\"server_id\":\"a1c9cf0c66c3ea102c600200d441ad8e\",\"version\":\"0.7.2\",\"go\":\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"tls_required\":false,\"tls_verify\":false,\"max_payload\":1048576}\r\n";
    // final BufferedOutputStream bw = mock(BufferedOutputStream.class);
    // final BufferedReader br = mock(BufferedReader.class);
    // byte[] pingBytes = "PING\r\n".getBytes();
    // try {
    // // When mock gets a PING, it should return a PONG
    // doAnswer(new Answer<Void>() {
    // @Override
    // public Void answer(InvocationOnMock invocation) throws Throwable {
    // when(br.readLine()).thenReturn("PONG");
    // return null;
    // }
    // }).when(bw).write(pingBytes, 0, pingBytes.length);
    //
    // when(br.readLine()).thenReturn(infoString);
    // } catch (IOException e) {
    // fail(e.getMessage());
    // }
    // TCPConnection mockConn = mock(TCPConnection.class);
    // when(mockConn.isConnected()).thenReturn(true);
    // when(mockConn.getBufferedReader()).thenReturn(br);
    // BufferedInputStream bis = mock(BufferedInputStream.class);
    // when(mockConn.getBufferedInputStream(ConnectionImpl.DEFAULT_STREAM_BUF_SIZE))
    // .thenReturn(bis);
    // when(mockConn.getBufferedOutputStream(any(int.class))).thenReturn(bw);
    //
    // try (ConnectionImpl c = new ConnectionFactory().createConnection(mockConn)) {
    // assertFalse(c.isClosed());
    // c.sendPing(null);
    // c.sendPing(null);
    //
    // } catch (IOException | TimeoutException e) {
    // e.printStackTrace();
    // fail("Unexpected exception: " + e.getMessage());
    // }
    // }

    @Test
    public void testSendSubscription() {
        String infoString =
                "INFO {\"server_id\":\"a1c9cf0c66c3ea102c600200d441ad8e\",\"version\":\"0.7.2\",\"go\":\"go1.4.2\",\"host\":\"0.0.0.0\",\"port\":4222,\"auth_required\":false,\"ssl_required\":false,\"tls_required\":false,\"tls_verify\":false,\"max_payload\":1048576}\r\n";
        final BufferedOutputStream bw = mock(BufferedOutputStream.class);
        final BufferedReader br = mock(BufferedReader.class);
        final BufferedInputStream bis = mock(BufferedInputStream.class);
        final TCPConnectionFactory mockTcf = mock(TCPConnectionFactory.class);

        byte[] pingBytes = "PING\r\n".getBytes();
        try {
            // When mock gets a PING, it should return a PONG
            doAnswer(new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    when(br.readLine()).thenReturn("PONG");
                    return null;
                }
            }).when(bw).write(pingBytes, 0, pingBytes.length);

            when(br.readLine()).thenReturn(infoString);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        try {
            when(bis.read(any(byte[].class), any(int.class), any(int.class)))
                    .thenAnswer(new Answer<Integer>() {

                        @Override
                        public Integer answer(InvocationOnMock invocation) {
                            UnitTestUtilities.sleep(5000);
                            return -1;
                        }
                    });
        } catch (IOException e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        }

        TCPConnection mockConn = mock(TCPConnection.class);

        when(mockConn.isConnected()).thenReturn(true);
        when(mockConn.getBufferedReader()).thenReturn(br);
        when(mockConn.getBufferedInputStream(ConnectionImpl.DEFAULT_STREAM_BUF_SIZE))
                .thenReturn(bis);
        when(mockConn.getBufferedOutputStream(any(int.class))).thenReturn(bw);

        SyncSubscriptionImpl sub = mock(SyncSubscriptionImpl.class);
        when(sub.getSubject()).thenReturn("foo");
        when(sub.getQueue()).thenReturn(null);
        when(sub.getSid()).thenReturn(1L);
        when(sub.getMaxPendingMsgs()).thenReturn(100);

        final AtomicBoolean exThrown = new AtomicBoolean(false);

        String s = String.format(ConnectionImpl.SUB_PROTO, sub.getSubject(),
                sub.getQueue() != null ? " " + sub.getQueue() : "", sub.getSid());

        byte[] bufToExpect = Utilities.stringToBytesASCII(s);
        try {
            doAnswer(new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    exThrown.set(true);
                    throw new IOException("Mock OutputStream sendSubscriptionMessage exception");
                }
            }).when(bw).write(bufToExpect);
        } catch (IOException e1) {
            fail(e1.getMessage());
        }

        when(mockTcf.createConnection()).thenReturn(mockConn);

        try (ConnectionImpl c = new ConnectionFactory().createConnection(mockTcf)) {
            c.sendSubscriptionMessage(sub);
            assertTrue("Should have thrown IOException", exThrown.get());
            exThrown.set(false);
            c.status = ConnState.RECONNECTING;
            c.sendSubscriptionMessage(sub);
            assertFalse("Should not have thrown IOException", exThrown.get());
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testAsyncSubscribeSubjQueue() {
        String subject = "foo";
        String queue = "bar";
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
            c.subscribeAsync(subject, queue);
            c.subscribeSync(subject, queue);
        } catch (IOException | TimeoutException e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testConnectionStatus() throws IOException, TimeoutException {
        try (Connection c = new ConnectionFactory().createConnection()) {
            assertEquals(ConnState.CONNECTED, c.getState());
            c.close();
            assertEquals(ConnState.CLOSED, c.getState());
        }
    }

    @Test
    public void testConnClosedCB() throws IOException, TimeoutException, InterruptedException {
        final CountDownLatch ccbLatch = new CountDownLatch(1);
        ConnectionFactory cf = new ConnectionFactory();
        cf.setClosedCallback(new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
                ccbLatch.countDown();
            }
        });
        try (Connection c = cf.createConnection()) {
            c.close();
        }
        assertTrue("Closed callback not triggered", ccbLatch.await(2, TimeUnit.SECONDS));
    }

    @Test
    public void testCloseDisconnectedHandler() throws IOException, TimeoutException {
        final AtomicBoolean disconnected = new AtomicBoolean(false);
        final Object disconnectedLock = new Object();

        ConnectionFactory cf = new ConnectionFactory();
        cf.setReconnectAllowed(false);
        cf.setDisconnectedCallback(new DisconnectedCallback() {
            @Override
            public void onDisconnect(ConnectionEvent event) {
                logger.trace("in disconnectedCB");
                synchronized (disconnectedLock) {
                    disconnected.set(true);
                    disconnectedLock.notify();
                }
            }
        });

        Connection c = cf.createConnection();
        assertFalse(c.isClosed());
        assertTrue(c.getState() == ConnState.CONNECTED);
        c.close();
        assertTrue(c.isClosed());
        synchronized (disconnectedLock) {
            try {
                disconnectedLock.wait(500);
                assertTrue("disconnectedCB not triggered.", disconnected.get());
            } catch (InterruptedException e) {
            }
        }

    }

    @Test
    public void testServerStopDisconnectedHandler() throws IOException, TimeoutException {
        final Lock disconnectLock = new ReentrantLock();
        final Condition hasBeenDisconnected = disconnectLock.newCondition();

        ConnectionFactory cf = new ConnectionFactory();
        cf.setReconnectAllowed(false);
        cf.setDisconnectedCallback(new DisconnectedCallback() {
            @Override
            public void onDisconnect(ConnectionEvent event) {
                disconnectLock.lock();
                try {
                    hasBeenDisconnected.signal();
                } finally {
                    disconnectLock.unlock();
                }
            }
        });

        try (Connection c = cf.createConnection()) {
            assertFalse(c.isClosed());
            disconnectLock.lock();
            try {
                bounceDefaultServer(1000);
                assertTrue(hasBeenDisconnected.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
            } finally {
                disconnectLock.unlock();
            }
        }
    }

    /// TODO NOT IMPLEMENTED:
    /// TestServerSecureConnections
    /// TestErrOnConnectAndDeadlock
    /// TestErrOnMaxPayloadLimit


    @Test
    public void testGetServerInfo() {
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            assertTrue(!c.isClosed());
            ServerInfo info = c.getConnectedServerInfo();
            assertEquals("0.0.0.0", info.getHost());
            assertEquals("0.7.2", info.getVersion());
            assertEquals(4222, info.getPort());
            assertFalse(info.isAuthRequired());
            assertFalse(info.isTlsRequired());
            assertEquals(1048576, info.getMaxPayload());
        } catch (IOException | TimeoutException e) {
            fail("Should not have thrown exception: " + e.getMessage());
        }
    }

    // @Test
    // public void testFlushFailureNoPong() {
    // try (TCPConnectionMock mock = new TCPConnectionMock())
    // {
    // boolean exThrown = false;
    // try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
    // assertFalse(c.isClosed());
    // exThrown = false;
    // try {
    // mock.setNoPongs(true);
    // c.flush(1000);
    // } catch (TimeoutException e) {
    //// System.err.println("timeout connection closed");
    // exThrown=true;
    // } catch (Exception e) {
    // fail("Wrong exception: " + e.getClass().getName());
    // }
    // assertTrue(exThrown);
    //
    // } catch (IOException | TimeoutException e) {
    // fail("Exception thrown");
    // }
    // } catch (Exception e) {
    // fail(e.getMessage());
    // }
    // }

    @Test
    public void testBadSid() {
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
            assertFalse(c.isClosed());
            try {
                TCPConnectionMock mock = (TCPConnectionMock) c.getTcpConnection();
                mock.deliverMessage("foo", 27, null, "Hello".getBytes());
            } catch (Exception e) {
                fail("Mock server shouldn't have thrown an exception: " + e.getMessage());
            }

        } catch (IOException | TimeoutException e) {
            fail(e.getMessage());
        }
    }

    // @Test
    // public void testDeliverMsgsChannelTimeout() {
    // try (TCPConnectionMock mock = new TCPConnectionMock()) {
    // try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
    // @SuppressWarnings("unchecked")
    // Channel<Message> ch = (Channel<Message>)mock(Channel.class);
    // when(ch.get()).
    // thenThrow(new TimeoutException("Timed out getting message from channel"));
    //
    // boolean timedOut=false;
    // try {
    // c.deliverMsgs(ch);
    // } catch (Error e) {
    // Throwable cause = e.getCause();
    // assertTrue(cause instanceof TimeoutException);
    // timedOut=true;
    // }
    // assertTrue("Should have thrown Error (TimeoutException)", timedOut);
    //
    // } catch (Exception e) {
    // e.printStackTrace();
    // fail(e.getMessage());
    // }
    // } catch (Exception e) {
    // fail(e.getMessage());
    // }
    // }

    @Test
    public void testProcessPing() {
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            BufferedOutputStream bw = mock(BufferedOutputStream.class);
            doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                    .write(any(byte[].class), any(int.class), any(int.class));
            doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                    .write(any(byte[].class));
            doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                    .write(any(int.class));
            c.setOutputStream(bw);
            c.processPing();
            assertTrue(c.getLastException() instanceof IOException);
            assertEquals("Mock OutputStream write exception", c.getLastException().getMessage());
        } catch (IOException | TimeoutException e) {
            fail("Connection attempt failed.");
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSendPingFailure() {
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            BufferedOutputStream bw = mock(BufferedOutputStream.class);
            doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                    .write(any(byte[].class), any(int.class), any(int.class));
            doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                    .write(any(byte[].class));
            doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                    .write(any(int.class));
            c.setOutputStream(bw);
            c.sendPing(new Channel<Boolean>());
            assertTrue(c.getLastException() instanceof IOException);
            assertEquals("Mock OutputStream write exception", c.getLastException().getMessage());
        } catch (IOException | TimeoutException e) {
            fail("Connection attempt failed.");
        }
    }

    @Test
    public void testProcessInfo() {
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            c.setConnectedServerInfo((ServerInfo) null);
            c.setConnectedServerInfo((String) null);
            assertNull(c.getConnectedServerInfo());
        } catch (IOException | TimeoutException e) {
            fail("Connection failed");
        }
    }

    @Test
    public void testUnsubscribe() {
        boolean exThrown = false;
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            SyncSubscriptionImpl s = (SyncSubscriptionImpl) c.subscribeSync("foo");
            long sid = s.getSid();
            assertNotNull("Sub should have been present", c.getSubs().get(sid));
            s.unsubscribe();
            assertNull("Sub should have been removed", c.getSubs().get(sid));
            c.close();
            assertTrue(c.isClosed());
            c.unsubscribe(s, 0);
        } catch (IllegalStateException e) {
            assertEquals("Unexpected exception: " + e.getMessage(), ERR_CONNECTION_CLOSED,
                    e.getMessage());
            exThrown = true;
        } catch (IOException | TimeoutException e) {
            fail("Unexpected exception");
        }
        assertTrue("Should have thrown IllegalStateException.", exThrown);
    }

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
    public void testPingTimer() {
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
            c.close();
            assertTrue(c.isClosed());
            c.processPingTimer();
        } catch (IOException | TimeoutException e) {
            fail("Connection failed");
        }

        ConnectionFactory cf = new ConnectionFactory();
        cf.setMaxPingsOut(0);
        cf.setReconnectAllowed(false);
        try (ConnectionImpl c = cf.createConnection(mcf)) {
            mcf.setNoPongs(true);
            BufferedOutputStream bw = mock(BufferedOutputStream.class);

            c.setOutputStream(bw);
            c.processPingTimer();
            assertTrue(c.isClosed());
            assertTrue(c.getLastException() instanceof IOException);
            assertEquals(ERR_STALE_CONNECTION, c.getLastException().getMessage());
        } catch (IOException | TimeoutException e) {
            fail("Connection failed");
        }
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
    public void testConnection() {
        ConnectionImpl ci = new ConnectionImpl();
        assertNotNull(ci);
    }

    @Test
    public void testNormalizeErr() {
        final String errString = "-ERR 'Authorization Violation'";
        ByteBuffer error = ByteBuffer.allocate(1024);
        error.put(errString.getBytes());
        error.flip();

        String s = ConnectionImpl.normalizeErr(error);
        assertEquals("authorization violation", s);
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
            Channel<Boolean> fch = c.getFlushChannel();
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
