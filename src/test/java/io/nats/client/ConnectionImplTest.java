/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.ConnectionImpl.DEFAULT_BUF_SIZE;
import static io.nats.client.ConnectionImpl.Srv;
import static io.nats.client.Nats.ConnState.CONNECTED;
import static io.nats.client.Nats.ConnState.RECONNECTING;
import static io.nats.client.Nats.ERR_BAD_SUBJECT;
import static io.nats.client.Nats.ERR_BAD_TIMEOUT;
import static io.nats.client.Nats.ERR_CONNECTION_CLOSED;
import static io.nats.client.Nats.ERR_CONNECTION_READ;
import static io.nats.client.Nats.ERR_MAX_PAYLOAD;
import static io.nats.client.Nats.ERR_NO_INFO_RECEIVED;
import static io.nats.client.Nats.ERR_NO_SERVERS;
import static io.nats.client.Nats.ERR_PROTOCOL;
import static io.nats.client.Nats.ERR_SECURE_CONN_REQUIRED;
import static io.nats.client.Nats.ERR_SECURE_CONN_WANTED;
import static io.nats.client.Nats.ERR_STALE_CONNECTION;
import static io.nats.client.Nats.ERR_TIMEOUT;
import static io.nats.client.Nats.defaultOptions;
import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.defaultInfo;
import static io.nats.client.UnitTestUtilities.newMockedConnection;
import static io.nats.client.UnitTestUtilities.newMockedTcpConnection;
import static io.nats.client.UnitTestUtilities.newMockedTcpConnectionFactory;
import static io.nats.client.UnitTestUtilities.setLogLevel;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.internal.verification.VerificationModeFactory.atLeast;

import ch.qos.logback.classic.Level;
import io.nats.client.ConnectionImpl.Control;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.URI;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import javax.net.ssl.SSLContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(UnitTest.class)
public class ConnectionImplTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static final Logger logger = LoggerFactory.getLogger(ConnectionImplTest.class);

    private static final LogVerifier verifier = new LogVerifier();

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @Mock
    private BlockingQueue<Boolean> fchMock;

    @Mock
    private BlockingQueue<Message> mchMock;

    @Mock
    private MessageHandler mcbMock;

    @Mock
    private Message msgMock;

    @Mock
    private BlockingQueue<Boolean> pongsMock;

    @Mock
    private BufferedOutputStream bwMock;

    @Mock
    private InputStream brMock;

    @Mock
    private ByteArrayOutputStream pendingMock;

    @Mock
    private ByteBuffer pubProtoBufMock;

    @Mock
    private ExecutorService cbExecMock;

    @Mock
    private Map<Long, SubscriptionImpl> subsMock;

    @Mock
    private SyncSubscriptionImpl syncSubMock;

    /**
     * @throws java.lang.Exception if a problem occurs
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception if a problem occurs
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception if a problem occurs
     */
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        verifier.setup();
    }

    /**
     * @throws java.lang.Exception if a problem occurs
     */
    @After
    public void tearDown() throws Exception {
        verifier.teardown();
        setLogLevel(Level.WARN);
    }

    @SuppressWarnings("resource")
    @Test
    public void testConnectionImpl() {
        new ConnectionImpl();
    }

    @Test
    public void testConnectProto() throws Exception {
        Options opts = defaultOptions();
        opts.url = "nats://derek:foo@localhost:4222";
        try (ConnectionImpl conn = new ConnectionImpl(opts)) {
            assertNotNull(conn.getUrl().getUserInfo());
            conn.setOutputStream(bwMock);
            String proto = conn.connectProto();
            ClientConnectInfo connectInfo = ClientConnectInfo.createFromWire(proto);
            assertEquals("derek", connectInfo.getUser());
            assertEquals("foo", connectInfo.getPass());
        }

        opts = Nats.defaultOptions();
        opts.url = "nats://thisismytoken@localhost:4222";
        try (ConnectionImpl conn = new ConnectionImpl(opts)) {
            assertNotNull(conn.getUrl().getUserInfo());
            conn.setOutputStream(bwMock);
            String proto = conn.connectProto();
            ClientConnectInfo connectInfo = ClientConnectInfo.createFromWire(proto);
            assertEquals("thisismytoken", connectInfo.getToken());
        }

    }

    @Test
    public void testGetPropertiesInputStreamFailures() throws IOException {
        ConnectionImpl conn = new ConnectionImpl(Nats.defaultOptions());
        conn.setOutputStream(bwMock);
        InputStream is = mock(InputStream.class);
        doThrow(new IOException("fake I/O exception")).when(is).read(any(byte[].class));

        conn.getProperties(is);
        verifier.verifyLogMsgEquals(Level.WARN, "nats: error loading properties from InputStream");

        Properties props = conn.getProperties((InputStream) null);
        assertNull(props);
        conn.close();
    }

    @Test
    public void testIsReconnecting() throws Exception {
        try (ConnectionImpl conn = new ConnectionImpl(Nats.defaultOptions())) {
            assertFalse(conn.isReconnecting());
            ConnectionAccessor.setState(conn, RECONNECTING);
            assertTrue(conn.isReconnecting());
            ConnectionAccessor.setState(conn, CONNECTED);
        }
    }

    @Test
    public void testSelectNextServerNoServers() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_SERVERS);

        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            List<Srv> pool = conn.getServerPool();
            pool.clear();
            conn.selectNextServer();
        }
    }

    @Test
    public void testSelectNextServer() throws Exception {
        Options opts = new Options.Builder().maxReconnect(-1).dontRandomize().build();
        opts.servers = Nats.processUrlString("nats://localhost:5222, nats://localhost:6222");
        ConnectionImpl conn = new ConnectionImpl(opts);
        final List<Srv> pool = conn.getServerPool();
        assertEquals("nats://localhost:5222", conn.currentServer().url.toString());

        Srv srv = conn.selectNextServer();
        assertEquals("nats://localhost:6222", srv.url.toString());
        conn.setUrl(srv.url);
        assertEquals(2, pool.size());
        assertEquals("nats://localhost:6222", pool.get(0).url.toString());
        assertEquals("nats://localhost:5222", pool.get(1).url.toString());

        srv = conn.selectNextServer();
        assertEquals("nats://localhost:5222", srv.url.toString());
        conn.setUrl(srv.url);
        assertEquals(2, pool.size());
        assertEquals("nats://localhost:5222", pool.get(0).url.toString());
        assertEquals("nats://localhost:6222", pool.get(1).url.toString());


        Options.Builder builder = new Options.Builder(conn.getOptions());
        ConnectionAccessor.setOptions(conn, builder.maxReconnect(1).build());
        srv.reconnects = 1;
        srv = conn.selectNextServer();
        assertEquals("nats://localhost:6222", srv.url.toString());
        conn.setUrl(srv.url);
        assertEquals(1, pool.size());
        assertEquals("nats://localhost:6222", pool.get(0).url.toString());

        conn.setOutputStream(mock(OutputStream.class));
        conn.close();
    }

    @Test
    public void testFlushReconnectPendingItems() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            byte[] pingProtoBytes = ConnectionImpl.PING_PROTO.getBytes();
            int pingProtoBytesLen = pingProtoBytes.length;
            byte[] pongProtoBytes = ConnectionImpl.PONG_PROTO.getBytes();
            int pongProtoBytesLen = pongProtoBytes.length;
            byte[] crlfProtoBytes = ConnectionImpl._CRLF_.getBytes();
            int crlfBytesLen = crlfProtoBytes.length;

            ByteArrayOutputStream baos = mock(ByteArrayOutputStream.class);
            when(baos.size()).thenReturn(pingProtoBytesLen);
            when(baos.toByteArray()).thenReturn(pingProtoBytes);

            assertNull(c.getPending());
            assertEquals(0, c.getPendingByteCount());

            // Test successful flush
            c.setPending(baos);
            c.setOutputStream(bwMock);
            c.flushReconnectPendingItems();
            verify(baos, times(1)).toByteArray();
            verify(bwMock, times(1)).write(eq(pingProtoBytes), eq(0), eq(pingProtoBytesLen));

            // Test with PING pending
            doThrow(new IOException("IOException from testFlushReconnectPendingItems")).when(bwMock)
                    .flush();

            c.setPending(baos);
            assertEquals(pingProtoBytesLen, c.getPendingByteCount());
            c.setOutputStream(bwMock);
            c.flushReconnectPendingItems();
            verifier.verifyLogMsgEquals(Level.ERROR, "Error flushing pending items");
            verify(baos, times(2)).toByteArray();
            verify(bwMock, times(2)).write(eq(pingProtoBytes), eq(0), eq(pingProtoBytesLen));
        }
    }

    @Test
    public void testFlushReconnectPendingNull() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            c.setPending(null);
            c.setOutputStream(bwMock);
            assertNull(c.getPending());

            // Test pending flush with null pending returns without exception
            c.flushReconnectPendingItems();
        }
    }

    @Test
    public void testReadLoopExitsIfConnClosed() throws Exception {
        Parser parserMock = mock(Parser.class);
        try (ConnectionImpl c = spy(new ConnectionImpl(Nats.defaultOptions()))) {
            c.setParser(parserMock);
            c.setInputStream(brMock);
            TcpConnection connMock = mock(TcpConnection.class);
            c.setTcpConnection(connMock);
            when(c.closed()).thenReturn(true);
            c.readLoop();
            assertNull(parserMock.ps);
            verify(brMock, times(0)).read(any(byte[].class));
        }
    }

    @Test
    public void testReadLoopExitsIfConnNull() throws Exception {
        Parser parserMock = mock(Parser.class);
        try (ConnectionImpl c = spy(new ConnectionImpl(Nats.defaultOptions()))) {
            c.setParser(parserMock);
            c.setInputStream(brMock);
            // TcpConnection connMock = mock(TcpConnection.class);
            c.setTcpConnection(null);
            c.readLoop();
            assertNull(parserMock.ps);
            verify(brMock, times(0)).read(any(byte[].class));
        }
    }

    @Test
    public void testReadLoopExitsIfReconnecting() throws Exception {
        try (ConnectionImpl conn = spy(new ConnectionImpl(Nats.defaultOptions()))) {
            conn.setInputStream(brMock);
            conn.setOutputStream(bwMock);
            TcpConnection connMock = mock(TcpConnection.class);
            conn.setTcpConnection(connMock);
            when(conn.reconnecting()).thenReturn(true);
            conn.readLoop();
            verify(brMock, times(0)).read(any(byte[].class));
        }
    }

    @Test
    public void testReadOp() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            BufferedReader brMock = c.getTcpConnection().getBufferedReader();
            when(brMock.readLine()).thenReturn("PING\r\n");
            Control pingLine = c.readOp();
            assertEquals(pingLine.op, "PING");
        }
    }

    /**
     * This test simulates what would happen if readLine() threw an exception in readOp(), which is
     * called by processExpectedInfo() when the TCP connection is first established.
     *
     * @throws Exception
     */
    @Test
    public void testReadOpThrows() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_CONNECTION_READ);

        TcpConnectionFactory mcf = mock(TcpConnectionFactory.class);
        TcpConnection tcpMock = newMockedTcpConnection();
        doReturn(tcpMock).when(mcf).createConnection();
        Options opts = new Options.Builder().factory(mcf).build();
        try (ConnectionImpl conn = spy(new ConnectionImpl(opts))) {
            doThrow(new IOException(ERR_CONNECTION_READ)).when(conn).readLine();
            // Connect will open the mock tcp connection, then call processExpectedInfo(), which
            // should result in a
            // connection read error.
            conn.connect();
            fail("Shouldn't have connected.");
        }
    }

    @Test
    public void testRemoveFlushEntry() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            ArrayList<BlockingQueue<Boolean>> pongs = new ArrayList<BlockingQueue<Boolean>>();
            c.setPongs(pongs);
            assertEquals(pongs, c.getPongs());
            // Basic case
            BlockingQueue<Boolean> testChan = new LinkedBlockingQueue<Boolean>();
            testChan.add(true);
            pongs.add(testChan);
            assertTrue("Failed to find chan", c.removeFlushEntry(testChan));

            BlockingQueue<Boolean> testChan2 = new LinkedBlockingQueue<Boolean>();
            testChan2.add(false);
            pongs.add(testChan);
            assertFalse("Should not have found chan", c.removeFlushEntry(testChan2));

            pongs.clear();
            assertFalse("Should have returned false", c.removeFlushEntry(testChan));
        }
    }

    @Test
    public void testRemoveFlushEntryReturnsWhenPongsNull() throws Exception {
        try (ConnectionImpl c = new ConnectionImpl(defaultOptions())) {
            c.setup();
            c.setPongs(null);
            BlockingQueue<Boolean> testChan = new LinkedBlockingQueue<Boolean>();
            assertFalse("Should return false", c.removeFlushEntry(testChan));
        }
    }

    @Test
    public void testDoReconnectSuccess() throws Exception {
        Options opts = new Options.Builder().reconnectWait(100).maxReconnect(1).build();

        try (ConnectionImpl conn = (ConnectionImpl) spy(newMockedConnection(opts))) {
            final CountDownLatch dcbLatch = new CountDownLatch(1);
            final CountDownLatch rcbLatch = new CountDownLatch(1);
            final AtomicInteger dcbCount = new AtomicInteger(0);
            final AtomicInteger rcbCount = new AtomicInteger(0);
            conn.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    dcbCount.incrementAndGet();
                    dcbLatch.countDown();
                }
            });
            conn.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    rcbCount.incrementAndGet();
                    rcbLatch.countDown();
                }
            });

            TcpConnection tconn = conn.getTcpConnection();
            tconn.close();

            assertTrue("DisconnectedCallback not triggered", dcbLatch.await(5, TimeUnit.SECONDS));
            assertEquals(1, dcbCount.get());
            assertTrue("ReconnectedCallback not triggered", rcbLatch.await(5, TimeUnit.SECONDS));
            assertEquals(1, rcbCount.get());
            verify(conn, times(0)).close();

        }
    }

    @Test
    public void testDoReconnectFlushFails() throws Exception {
        final OutputStream mockOut =
                mock(OutputStream.class);
        TcpConnectionFactory tcf = newMockedTcpConnectionFactory();
        TcpConnection tconn = newMockedTcpConnection();
        Options opts = new Options.Builder().reconnectWait(100).maxReconnect(1).factory(tcf)
                .build();
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection(opts)) {
            final CountDownLatch latch = new CountDownLatch(1);
            conn.setClosedCallback(new ClosedCallback() {
                @Override
                public void onClose(ConnectionEvent event) {
                    latch.countDown();
                }
            });
            doReturn(tconn).when(tcf).createConnection();
            doReturn(mockOut).when(tconn).getOutputStream(anyInt());
            doNothing().doNothing().doThrow(new IOException("FOO")).when(mockOut).flush();

            doNothing().doThrow(new SocketException("Connection refused")).when(tconn).open(anyString(), anyInt());
            conn.getTcpConnection().close();

            assertTrue(latch.await(5, TimeUnit.SECONDS));

            verifier.verifyLogMsgEquals(Level.WARN, "Error flushing output stream", atLeastOnce());
        }
    }

    @Test
    // FIXME tests like this will create a real NATS connection on reconnect. Fix all of them
    public void testDoReconnectNoServers()
            throws Exception {
        Options opts = new Options.Builder().reconnectWait(1).build();
        try (ConnectionImpl c =
                     Mockito.spy(new ConnectionImpl(opts))) {
            c.setOutputStream(bwMock);
            c.setPending(pendingMock);
            doThrow(new IOException(ERR_NO_SERVERS)).when(c).selectNextServer();
            c.doReconnect();
            assertTrue(c.getLastException() instanceof IOException);
            assertEquals(ERR_NO_SERVERS, c.getLastException().getMessage());
        }
    }

    @Test
    public void testDoReconnectCreateConnFailed()
            throws Exception {
        Options opts = new Options.Builder().reconnectWait(1).maxReconnect(1).build();
        try (ConnectionImpl c =
                     Mockito.spy(new ConnectionImpl(opts))) {
            c.setOutputStream(bwMock);
            c.setPending(pendingMock);
            doThrow(new IOException(ERR_NO_SERVERS)).when(c).createConn();
            c.doReconnect();
            assertTrue(c.getLastException() instanceof IOException);
            assertEquals(ERR_NO_SERVERS, c.getLastException().getMessage());
        }
    }

    @Test
    public void testDoReconnectProcessConnInitFailed() throws Exception {
        Options opts = new Options.Builder().factory(newMockedTcpConnectionFactory())
                .reconnectWait(1).maxReconnect(1).build();
        try (ConnectionImpl conn = spy(new ConnectionImpl(opts))) {
            conn.setOutputStream(bwMock);
            conn.setPending(pendingMock);

            doThrow(new IOException(ERR_PROTOCOL + ", INFO not received")).when(conn)
                    .processExpectedInfo();
            conn.doReconnect();
            verify(conn, times(1)).processConnectInit();
            verifier.verifyLogMsgMatches(Level.WARN, "couldn't connect to .+$");
        }
    }

    @Test
    public void testDoReconnectConnClosed()
            throws Exception {
        Options opts = new Options.Builder(Nats.defaultOptions())
                .reconnectWait(1)
                .maxReconnect(1).build();
        try (ConnectionImpl c =
                     Mockito.spy(new ConnectionImpl(opts))) {
            c.setOutputStream(bwMock);
            c.setPending(pendingMock);
            when(c.isClosed()).thenReturn(true);
            c.doReconnect();
            assertEquals(ERR_NO_SERVERS, c.getLastException().getMessage());
        }
    }

    @Test
    public void testDoReconnectCallsReconnectedCb()
            throws Exception {
        Options opts = new Options.Builder().reconnectWait(1).maxReconnect(1).build();
        // setLogLevel(Level.TRACE);
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection(opts)) {
            TcpConnection tconn = c.getTcpConnection();
            final AtomicInteger rcbCount = new AtomicInteger(0);
            final CountDownLatch rcbLatch = new CountDownLatch(1);
            assertTrue(c.getOptions().isReconnectAllowed());
            ReconnectedCallback rcb = new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    rcbCount.incrementAndGet();
                    rcbLatch.countDown();
                }
            };
            c.setReconnectedCallback(rcb);
            assertEquals(rcb, c.getReconnectedCallback());
            assertEquals(rcb, c.getOptions().getReconnectedCallback());
            assertEquals(rcb, c.getOptions().reconnectedCb);

            tconn.close();

            assertTrue(rcbLatch.await(5, TimeUnit.SECONDS));
            assertEquals(1, rcbCount.get());
        }
    }

    @Test
    public void testProcessAsyncInfo() throws Exception {
        ServerInfo info = ServerInfo.createFromWire(UnitTestUtilities.defaultAsyncInfo);
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            System.err.println("Test calling processAsyncInfo");
            byte[] asyncInfo = UnitTestUtilities.defaultAsyncInfo.getBytes();
            c.processAsyncInfo(asyncInfo, 0, asyncInfo.length);
            assertEquals(info, c.getConnectedServerInfo());
        }
    }

    @Test
    public void testProcessAsyncInfoFinallyBlock() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("test message");
        try (ConnectionImpl c = (ConnectionImpl) spy(newMockedConnection())) {
            doThrow(new IllegalStateException("test message")).when(c)
                    .processInfo(eq(UnitTestUtilities.defaultAsyncInfo.trim()));
            byte[] asyncInfo = UnitTestUtilities.defaultAsyncInfo.getBytes();
            c.processAsyncInfo(asyncInfo, 0, asyncInfo.length);
        }
    }

    @Test
    public void testProcessInfo() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            ServerInfo info = ServerInfo.createFromWire(UnitTestUtilities.defaultInfo);
            c.processInfo(UnitTestUtilities.defaultInfo.trim());
            assertEquals(info, c.getConnectedServerInfo());
        }
    }

    @Test
    public void testProcessInfoWithConnectUrls() throws Exception {
        ServerInfo info = ServerInfo.createFromWire(UnitTestUtilities.defaultAsyncInfo);
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            c.processInfo(UnitTestUtilities.defaultAsyncInfo.trim());
            assertEquals(info, c.getConnectedServerInfo());
        }
    }

    @Test
    public void testProcessInfoNullOrEmptyReturnsEarly() throws Exception {
        try (ConnectionImpl nc =
                     Mockito.spy(new ConnectionImpl(Nats.defaultOptions()))) {
            nc.setOutputStream(bwMock);
            nc.processInfo(null);
            verify(nc, times(0)).addUrlToPool(any(String.class), eq(true));
            nc.processInfo("");
            verify(nc, times(0)).addUrlToPool(any(String.class), eq(true));
        }
    }

    @Test
    public void testProcessOpErrConditionsNotMet() throws Exception {
        Options opts = spy(new Options.Builder(Nats.defaultOptions()).build());
        try (ConnectionImpl c = Mockito.spy(new ConnectionImpl(opts))) {
            when(c.connecting()).thenReturn(true);
            assertTrue(c.connecting());

            c.processOpError(new Exception("foo"));
            verify(opts, times(0)).isReconnectAllowed();

            when(c.connecting()).thenReturn(false);
            when(c.closed()).thenReturn(true);
            c.processOpError(new Exception("foo"));
            verify(opts, times(0)).isReconnectAllowed();


            when(c.connecting()).thenReturn(false);
            when(c.closed()).thenReturn(false);
            when(c.reconnecting()).thenReturn(true);
            c.processOpError(new Exception("foo"));
            verify(opts, times(0)).isReconnectAllowed();
        }
    }

    @Test
    public void testProcessOpErrFlushError() throws Exception {
        try (ConnectionImpl conn = new ConnectionImpl(defaultOptions())) {
            conn.setup();
            conn.setOutputStream(bwMock);
            doThrow(new IOException("testProcessOpErrFlushError()")).when(bwMock).flush();
            conn.setTcpConnection(newMockedTcpConnection());
            ConnectionAccessor.setState(conn, CONNECTED);

            conn.processOpError(new Exception("foo"));
            verifier.verifyLogMsgEquals(Level.WARN, "I/O error during flush");
        }
    }

    @Test
    public void testProcessPingError() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) spy(newMockedConnection())) {
            Exception ex = new IOException("testProcessPingError()");
            doThrow(ex).when(c).sendProto(any(byte[].class), anyInt());
            c.processPing();
            verify(c, times(1)).setLastError(eq(ex));
        }
    }

    @Test
    public void testPublishIoError() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            Message msg = new Message();
            msg.setSubject("foo");
            msg.setData(null);
            BufferedOutputStream bw = mock(BufferedOutputStream.class);
            doThrow(new IOException("Mock OutputStream write exception")).when(bw)
                    .write(any(byte[].class), any(int.class), any(int.class));
            c.setOutputStream(bw);
            c.publish(msg);
            assertTrue(c.getLastException() instanceof IOException);
            assertEquals("Mock OutputStream write exception", c.getLastException().getMessage());
        }
    }


    @Test
    public void testPublishBadSubject() throws Exception {
        try (Connection c = newMockedConnection()) {
            boolean exThrown = false;
            try {
                c.publish("", null);
            } catch (IllegalArgumentException e) {
                assertEquals(ERR_BAD_SUBJECT, e.getMessage());
                exThrown = true;
            } finally {
                assertTrue(exThrown);
            }
            exThrown = false;
            try {
                c.publish(null, "Hello".getBytes());
            } catch (NullPointerException e) {
                assertEquals(ERR_BAD_SUBJECT, e.getMessage());
                exThrown = true;
            } finally {
                assertTrue(exThrown);
            }
        }
    }

    @Test
    public void testNewMockConn() throws Exception {
        Options opts = Nats.defaultOptions();
        try (Connection nc = UnitTestUtilities.newMockedConnection(opts)) {
            assertFalse(nc.isClosed());
        }
    }

    @Test
    public void testPublishWithReply() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            c.publish("foo", "bar", null);
            verify(c, times(1)).publish(eq("foo".getBytes()), eq("bar".getBytes()),
                    eq((byte[]) null), any(boolean.class));
        }
    }

    @Test
    public void testResendSubscriptions() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) spy(newMockedConnection())) {
            AsyncSubscriptionImpl sub =
                    (AsyncSubscriptionImpl) c.subscribe("foo", new MessageHandler() {
                        public void onMessage(Message msg) {
                            System.err.println("got msg: " + msg);
                        }
                    });
            // First, test the case where we have reached max, therefore we're going to unsubscribe.
            sub.setMax(122);
            assertEquals(122, sub.max);
            sub.delivered = 122;
            assertEquals(sub.getMax(), sub.getDelivered());
            c.resendSubscriptions();
            verify(c).unsubscribe(sub, 0);

            SyncSubscriptionImpl syncSub = (SyncSubscriptionImpl) c.subscribeSync("foo");
            syncSub.setMax(10);
            syncSub.delivered = 8;
            long adjustedMax = (syncSub.getMax() - syncSub.getDelivered());
            assertEquals(2, adjustedMax);
            c.resendSubscriptions();
            verify(c).writeUnsubProto(syncSub, adjustedMax);

        }
    }


    @Test
    public void testErrOnMaxPayloadLimit() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(ERR_MAX_PAYLOAD);

        long expectedMaxPayload = 10;

        ServerInfo info = ServerInfo.createFromWire(defaultInfo);
        info.setMaxPayload(expectedMaxPayload);

        TcpConnectionFactory mcf = newMockedTcpConnectionFactory(info);
        Options opts = new Options.Builder().factory(mcf).build();
        try (Connection c = opts.connect()) {
            // Make sure we parsed max payload correctly
            assertEquals(c.getMaxPayload(), expectedMaxPayload);

            c.publish("hello", "hello world".getBytes());

            // Check for success on less than maxPayload

        }
    }

    @Test
    public void testGetConnectedServerId() throws Exception {
        final String expectedId = "a1c9cf0c66c3ea102c600200d441ad8e";
        try (Connection c = newMockedConnection()) {
            assertTrue(!c.isClosed());
            assertEquals("Wrong server ID", c.getConnectedServerId(), expectedId);
            c.close();
            assertNull("Should have returned NULL", c.getConnectedServerId());
        }
    }

    @Test
    public void testGetConnectedServerInfo() throws Exception {
        try (Connection nc = newMockedConnection()) {
            assertTrue(!nc.isClosed());
            String expected = UnitTestUtilities.defaultInfo;
            ServerInfo actual = nc.getConnectedServerInfo();
            assertEquals("Wrong server INFO.", expected, actual.toString());
            assertTrue(actual.equals(ServerInfo.createFromWire(expected)));
        }
    }

    @Test
    public void testPublishClosedConnection() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(Nats.ERR_CONNECTION_CLOSED);
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            c.close();
            c.publish("foo", null);
        }
    }

    @Test
    public void testPublishReconnectingFlushError() throws Exception {
        OutputStream os = mock(OutputStream.class);
        doThrow(new IOException("test")).when(os).flush();
        try (ConnectionImpl connection = new ConnectionImpl(defaultOptions())) {
            connection.setOutputStream(os);
            connection.setConnectedServerInfo(ServerInfo.createFromWire(defaultInfo));
            connection.setPending(mock(ByteArrayOutputStream.class));
            connection.setFlushChannel(fchMock);
            ConnectionAccessor.setState(connection, RECONNECTING);
            connection.publish("foo", null);
            verifier.verifyLogMsgEquals(Level.ERROR, "I/O exception during flush");
        }
    }

    @Test
    public void testPublishReconnectingPendingBufferTooLarge()
            throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(Nats.ERR_RECONNECT_BUF_EXCEEDED);

        try (ConnectionImpl conn = new ConnectionImpl(defaultOptions())) {
            conn.setConnectedServerInfo(ServerInfo.createFromWire(defaultInfo));
            conn.setOutputStream(bwMock);
            conn.setPending(pendingMock);
            when(pendingMock.size()).thenReturn(conn.getOptions().getReconnectBufSize() + 10);
            ConnectionAccessor.setState(conn, RECONNECTING);
            conn.publish("foo", null);
            verifier.verifyLogMsgEquals(Level.ERROR, "I/O exception during flush");
        }
    }

    @Test
    public void testPublishBufferOverflow() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) spy(newMockedConnection())) {
            // This will cause writePublishProto to throw an exception on first invocation but not
            // on the second invocation
            doThrow(new BufferOverflowException()).doNothing().when(c).writePublishProto(
                    any(java.nio.ByteBuffer.class), eq("foo".getBytes()), eq((byte[]) null), eq(0));
            c.setOutputStream(bwMock);
            c.setPending(pendingMock);
            c.publish("foo", null);
            verifier.verifyLogMsgEquals(Level.WARN,
                    "nats: reallocating publish buffer due to overflow");
        }
    }

    @Test
    public void testGetPropertiesStringFailure() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            Properties props = c.getProperties("foobar.properties");
            assertNull(props);
        }
    }

    @Test
    public void testGetPropertiesSuccess() {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            Properties props = c.getProperties("jnats.properties");
            assertNotNull(props);
            String version = props.getProperty("client.version");
            assertNotNull(version);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testConnectThrowsConnectionRefused() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_SERVERS);
        try (ConnectionImpl c = spy(new ConnectionImpl(Nats.defaultOptions()))) {
            doThrow(new SocketException("Connection refused")).when(c).createConn();
            c.connect();
            // When createConn() throws "connection refused", setup() should not be called
            verify(c, times(0)).setup();
            // When createConn() throws "connection refused", setLastError(null) should happen
            verify(c, times(1)).setLastError(eq((Exception) null));
        }
    }

    @Test
    public void testCreateConnCurrentSrvNull() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_SERVERS);

        try (ConnectionImpl c = spy(new ConnectionImpl(Nats.defaultOptions()))) {
            when(c.currentServer()).thenReturn(null);
            c.createConn();
        }
    }

    @Test
    public void testCreateConnFlushFailure() throws Exception {
        try (ConnectionImpl c = spy(new ConnectionImpl(Nats.defaultOptions()))) {
            c.setTcpConnectionFactory(newMockedTcpConnectionFactory());
            c.setOutputStream(bwMock);
            c.setPending(pendingMock);
            doThrow(new IOException("testCreateConnFlushFailure()")).when(bwMock).flush();
            when(c.currentServer())
                    .thenReturn(new Srv(URI.create("nats://localhost:4222"), false));
            c.createConn();
            verify(bwMock, times(1)).flush();
            verifier.verifyLogMsgEquals(Level.WARN, Nats.ERR_TCP_FLUSH_FAILED);
        }
    }

    @Test
    public void testCreateConnOpenFailure() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_SERVERS);

        TcpConnectionFactory tcf = newMockedTcpConnectionFactory();
        Options opts = new Options.Builder(Nats.defaultOptions()).factory(tcf).build();
        try (ConnectionImpl nc = new ConnectionImpl(opts)) {
            TcpConnection conn = tcf.createConnection();
            doThrow(new IOException(ERR_NO_SERVERS)).when(conn).open(anyString(), anyInt());
            doReturn(conn).when(tcf).createConnection();
            setLogLevel(Level.DEBUG);
            nc.createConn();
            verifier.verifyLogMsgMatches(Level.DEBUG, "Couldn't establish connection to .+$");
        }
    }

    @Test
    public void testClosedCallback() throws Exception {
        final CountDownLatch ccbLatch = new CountDownLatch(1);
        final AtomicInteger ccbCount = new AtomicInteger(0);
        ClosedCallback ccb = new ClosedCallback() {
            public void onClose(ConnectionEvent event) {
                ccbCount.incrementAndGet();
                ccbLatch.countDown();
            }
        };
        try (Connection c = newMockedConnection()) {
            c.setClosedCallback(ccb);
            assertEquals(ccb, c.getClosedCallback());
        }

        assertTrue(ccbLatch.await(2, TimeUnit.SECONDS));
        assertEquals(1, ccbCount.get());
    }

    @Test
    public void testClosedConnections() throws Exception {
        Connection nc = newMockedConnection();
        SyncSubscription sub = nc.subscribeSync("foo");

        nc.close();
        assertTrue(nc.isClosed());

        // While we can annotate all the exceptions in the test framework,
        // just do it manually.

        boolean exThrown = false;

        try {
            nc.publish("foo", null);
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        assertTrue(nc.isClosed());
        try {
            nc.publish(new Message("foo", null, null));
        } catch (Exception e) {
            exThrown = true;
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            nc.subscribeSync("foo");
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            nc.subscribeSync("foo", "bar");
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            nc.request("foo", null);
            assertTrue(nc.isClosed());
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            sub.nextMessage();
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            sub.nextMessage(100);
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            sub.unsubscribe();
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        try {
            sub.autoUnsubscribe(1);
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }
    }

    @Test
    public void testCloseNullTcpConn() throws Exception {
        // Null TCP connection and null disconnected callback
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            conn.setTcpConnection(null);
            conn.setOutputStream(bwMock);
            assertNull(conn.getDisconnectedCallback());
            conn.close();
            // If the TCP conn is null, we should not attempt to flush
            verify(bwMock, times(0)).flush();
            // And we should not execute the dcb, or an exception will be thrown during close()
        }

        // Null TCP connection and valid disconnected callback
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            final AtomicInteger dcbCount = new AtomicInteger(0);
            conn.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    dcbCount.incrementAndGet();
                }
            });
            conn.setTcpConnection(null);
            conn.setOutputStream(bwMock);
            conn.close();
            // If the TCP conn is null, we should not attempt to flush
            verify(bwMock, times(0)).flush();
            // And we should not call the dcb
            assertTrue(conn.isClosed());
            assertEquals(0, dcbCount.get());
        }

        // Test for valid TCP connection and valid disconnected callback
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            final CountDownLatch dcbLatch = new CountDownLatch(1);
            conn.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    dcbLatch.countDown();
                }
            });
            conn.setOutputStream(bwMock);
            conn.close();
            // If the TCP conn is null, we should have flushed
            verify(bwMock, times(1)).flush();
            // And we should have called the dcb
            assertTrue(dcbLatch.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testExhaustedSrvPool() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_SERVERS);

        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            List<Srv> pool = new ArrayList<Srv>();
            c.setServerPool(pool);
            assertNull(c.currentServer());
            c.createConn();
        }
    }


    @Test
    public void testFlushBadTimeout() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(ERR_BAD_TIMEOUT);
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            c.flush(-10);
        }
    }

    @Test
    public void testFlushTimeoutFailure() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_TIMEOUT);

        int timeout = 500;
        TimeUnit unit = TimeUnit.MILLISECONDS;
        try (ConnectionImpl connection = (ConnectionImpl) spy(newMockedConnection())) {
            doReturn(fchMock).when(connection).createBooleanChannel(1);
            Boolean result = doReturn(null).when(fchMock).poll(timeout, unit);
            connection.flush((int) unit.toMillis(timeout));
        }
    }

    @Test
    public void testFlushClosedFailure() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_CONNECTION_CLOSED);
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            conn.close();
            conn.flush(500);
        }
    }

    @Test
    public void testFlushFalsePongFailure() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_CONNECTION_CLOSED);
        @SuppressWarnings("unchecked")
        BlockingQueue<Boolean> ch = (BlockingQueue<Boolean>) mock(BlockingQueue.class);
        try (ConnectionImpl conn = spy(new ConnectionImpl(defaultOptions()))) {
            conn.setup();
            conn.setOutputStream(bwMock);
            doReturn(ch).when(conn).createBooleanChannel(1);
            doReturn(false).when(ch).poll(100, TimeUnit.MILLISECONDS);
            conn.flush(100);
        }
    }

    @Test
    public void testFlushPollInterrupted() throws Exception {
        thrown.expect(InterruptedException.class);
        thrown.expectMessage("FOO");
        @SuppressWarnings("unchecked")
        BlockingQueue<Boolean> ch = (BlockingQueue<Boolean>) mock(BlockingQueue.class);
        try (ConnectionImpl c = Mockito.spy(new ConnectionImpl(defaultOptions()))) {
            c.setOutputStream(bwMock);
            doReturn(ch).when(c).createBooleanChannel(1);
            doThrow(new InterruptedException("FOO")).when(ch).poll(500, TimeUnit.MILLISECONDS);
            c.flush(500);
        }
    }


    @Test
    public void testWaitForMsgsSuccess()
            throws Exception {
        final String subj = "foo";
        final byte[] payload = "Hello there!".getBytes();
        try (ConnectionImpl nc = spy(new ConnectionImpl(defaultOptions()))) {
            final AsyncSubscriptionImpl sub = mock(AsyncSubscriptionImpl.class);
            final Message msg = new Message(payload, subj, null, sub);
            when(sub.getSid()).thenReturn(14L);
            @SuppressWarnings("unchecked")
            BlockingQueue<Message> ch = (BlockingQueue<Message>) mock(BlockingQueue.class);
            doReturn(1).when(ch).size();
            doReturn(msg, (Message) null).when(ch).take();
            sub.mch = ch;

            MessageHandler mcbMock = mock(MessageHandler.class);
            doReturn(mcbMock).when(sub).getMessageHandler();
            sub.setMessageHandler(mcbMock);

            sub.setChannel(mchMock);
            doReturn(1, 0).when(mchMock).size();
            doReturn(msg, (Message) null).when(mchMock).poll();
            doReturn(mchMock).when(sub).getChannel();

            sub.pCond = mock(Condition.class);

            sub.max = 1; // To make sure the message is removed after one

            doReturn(false, true).when(sub).isClosed();
            nc.waitForMsgs(sub);

            // verify the msg was processed
            verify(mcbMock, times(1)).onMessage(msg);

            // verify the sub was removed
            verify(nc, times(1)).removeSub(eq(sub));
        }
    }

    @Test
    public void testProcessErr() throws Exception {
        String err = "this is a test";
        // byte[] argBufBase = new byte[DEFAULT_BUF_SIZE];
        ByteBuffer errorStream = ByteBuffer.wrap(err.getBytes());

        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            assertTrue(!c.isClosed());
            c.processErr(errorStream);
            assertTrue(c.isClosed());
            NATSException ex = (NATSException) c.getLastException();
            assertNotNull(ex);
            String msg = ex.getMessage();
            assertNotNull(msg);
            assertEquals("nats: " + err, msg);
            Connection exConn = ex.getConnection();
            assertNotNull(exConn);
            assertEquals(c, exConn);
        }
    }

    @Test
    public void testProcessErrStaleConnection() throws Exception {
        final CountDownLatch ccbLatch = new CountDownLatch(1);
        ClosedCallback ccb = new ClosedCallback() {
            @Override
            public void onClose(ConnectionEvent event) {
                // logger.info("CLOSED CONNECTION");
                ccbLatch.countDown();
            }
        };
        Options opts = new Options.Builder().noReconnect().closedCb(ccb)
                .factory(newMockedTcpConnectionFactory()).build();
        try (ConnectionImpl c = (ConnectionImpl) opts.connect()) {
            ByteBuffer error = ByteBuffer.allocate(DEFAULT_BUF_SIZE);
            error.put(ConnectionImpl.STALE_CONNECTION.getBytes());
            error.flip();
            c.processErr(error);
            assertTrue(c.isClosed());
            assertTrue("Closed callback should have fired", await(ccbLatch));
        }
    }

    @Test
    public void testProcessErrPermissionsViolation() throws Exception {
        final String serverError = Nats.PERMISSIONS_ERR + " foobar";
        final String errMsg = "nats: " + serverError;
        final CountDownLatch ecbLatch = new CountDownLatch(1);
        final AtomicBoolean correctAsyncError = new AtomicBoolean();

        Options opts = new Options.Builder().factory(newMockedTcpConnectionFactory())
                .errorCb(new ExceptionHandler() {
                    @Override
                    public void onException(NATSException ex) {
                        Throwable cause = ex.getCause();
                        if (cause.getMessage().equals(errMsg)) {
                            correctAsyncError.set(true);
                        }
                        ecbLatch.countDown();
                    }
                }).noReconnect().build();
        try (ConnectionImpl conn = (ConnectionImpl) spy(opts.connect())) {
            ByteBuffer error = (ByteBuffer) ByteBuffer.allocate(DEFAULT_BUF_SIZE)
                    .put(serverError.getBytes())
                    .flip();
            conn.processErr(error);

            // Verify we identified and processed it
            verify(conn, times(1)).processPermissionsViolation(serverError);

            // wait for callback
            assertTrue("Async error cb should have been triggered", ecbLatch.await(5, TimeUnit
                    .SECONDS));

            // verify callback got correct error
            assertTrue("Should get correct error", correctAsyncError.get());

            // verufy conn's error was also set correctly
            Exception err = conn.getLastException();
            assertNotNull(err);
            assertEquals(errMsg, err.getMessage());

        }
    }

    @Test
    public void testProcessPermissionsViolation() throws Exception {
        final CountDownLatch ehLatch = new CountDownLatch(1);
        final String errorString = "some error";
        try (ConnectionImpl conn = (ConnectionImpl) spy(newMockedConnection())) {
            conn.setExceptionHandler(new ExceptionHandler() {
                @Override
                public void onException(NATSException e) {
                    assertEquals(conn, e.getConnection());
                    Exception lastEx = conn.getLastException();
                    assertNotNull(lastEx);
                    assertNotNull(e.getCause());
                    // Our cause should be the same as the last exception registered on the
                    // connection
                    assertEquals(lastEx, e.getCause());

                    assertNotNull(e.getCause().getMessage());
                    assertEquals("nats: " + errorString, e.getCause().getMessage());
                    ehLatch.countDown();
                }
            });

            conn.processPermissionsViolation(errorString);

            // Verify we identified and processed it
            verify(conn, times(1)).processPermissionsViolation(errorString);
        }
    }

    @Test
    public void testProcessExpectedInfoThrowsErrNoInfoReceived()
            throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_INFO_RECEIVED);

        TcpConnectionFactory mcf = newMockedTcpConnectionFactory();
        TcpConnection tcpMock = newMockedTcpConnection();
        doReturn(tcpMock).when(mcf).createConnection();
        BufferedReader br = tcpMock.getBufferedReader();
        doReturn("FOOBAR").when(br).readLine();
        Options opts = new Options.Builder().factory(mcf).build();
        try (Connection c = newMockedConnection(opts)) {
            fail("Shouldn't have connected.");
        }
    }

    @Test
    public void testProcessExpectedInfoReadOpFailure() throws Exception {
        try (ConnectionImpl conn = (ConnectionImpl) spy(newMockedConnection())) {
            TcpConnection connMock = mock(TcpConnection.class);
            BufferedReader brMock = mock(BufferedReader.class);
            when(connMock.getBufferedReader()).thenReturn(brMock);
            conn.setTcpConnection(connMock);
            doThrow(new IOException("flush error")).when(brMock).readLine();
            conn.processExpectedInfo();
            verify(conn, times(1)).processOpError(any(IOException.class));
            verify(conn, times(0)).processInfo(anyString());
        }
    }

    @Test
    public void testProcessMsg() throws Exception {
        final byte[] data = "Hello, World!".getBytes();
        final int offset = 0;
        final int length = data.length;
        try (ConnectionImpl c = (ConnectionImpl) spy(new ConnectionImpl(defaultOptions()))) {
            c.setup();
            c.setOutputStream(bwMock);
            Parser parser = c.getParser();
            SubscriptionImpl sub = (SubscriptionImpl) Mockito.spy(c.subscribe("foo", mcbMock));
            parser.ps.ma.sid = 44L;
            when(subsMock.get(any(long.class))).thenReturn(sub);
            when(mchMock.add(any(Message.class))).thenReturn(true);
            c.setSubs(subsMock);
            sub.setChannel(mchMock);
            sub.pCond = mock(Condition.class);

            parser.ps.ma.size = length;
            c.processMsg(data, offset, length);

            // InMsgs should be incremented by 1
            assertEquals(1, c.getStats().getInMsgs());
            // InBytes should be incremented by length
            assertEquals(length, c.getStats().getInBytes());
            // sub.addMessage(msg) should have been called exactly once
            verify(mchMock, times(1)).add(any(Message.class));
            // condition should have been signaled
            verify(sub.pCond, times(1)).signal();
            // sub.setSlowConsumer(false) should have been called
            verify(sub, times(1)).setSlowConsumer(eq(false));
            // c.removeSub should NOT have been called
            verify(c, times(0)).removeSub(eq(sub));
        }
    }

    @Test
    public void testProcessMsgMaxReached() throws Exception {
        final byte[] data = "Hello, World!".getBytes();
        final int offset = 0;
        final int length = data.length;
        final long sid = 4L;
        try (ConnectionImpl c = (ConnectionImpl) spy(newMockedConnection())) {
            Parser parser = c.getParser();
            SubscriptionImpl sub = spy(new AsyncSubscriptionImpl(c, "foo", "bar", mcbMock));
            when(sub.getSid()).thenReturn(sid);
            parser.ps.ma.sid = sid;
            parser.ps.ma.size = length;
            when(subsMock.get(eq(sid))).thenReturn(sub);
            assertEquals(sub, subsMock.get(sid));
            c.setSubs(subsMock);

            sub.setPendingLimits(1, 1024);
            sub.pMsgs = 1;

            assertEquals(1, sub.getPendingMsgsLimit());
            assertEquals(1, sub.pMsgs);

            c.processMsg(data, offset, length);

            // InMsgs should be incremented by 1, even if the sub stats don't increase
            assertEquals(1, c.getStats().getInMsgs());
            // InBytes should be incremented by length, even if the sub stats don't increase
            assertEquals(length, c.getStats().getInBytes());
            // handleSlowConsumer should have been called once
            verify(c, times(1)).handleSlowConsumer(eq(sub), any(Message.class));
            // sub.addMessage(msg) should not have been called
            verify(mchMock, times(0)).add(any(Message.class));
            // sub.setSlowConsumer(false) should NOT have been called
            verify(sub, times(0)).setSlowConsumer(eq(false));
        }
    }

    @Test
    public void testProcessMsgSubChannelAddFails() throws Exception {
        final byte[] data = "Hello, World!".getBytes();
        final int offset = 0;
        final int length = data.length;
        final long sid = 4L;
        try (ConnectionImpl c = (ConnectionImpl) spy(newMockedConnection())) {
            Parser parser = c.getParser();
            SubscriptionImpl sub = spy(new AsyncSubscriptionImpl(c, "foo", "bar", mcbMock));
            when(sub.getSid()).thenReturn(sid);
            parser.ps.ma.sid = sid;
            parser.ps.ma.size = length;
            when(subsMock.get(eq(sid))).thenReturn(sub);
            assertEquals(sub, subsMock.get(sid));
            c.setSubs(subsMock);

            when(mchMock.add(any(Message.class))).thenReturn(false);
            when(sub.getChannel()).thenReturn(mchMock);

            sub.pCond = mock(Condition.class);

            c.processMsg(data, offset, length);

            // InMsgs should be incremented by 1, even if the sub stats don't increase
            assertEquals(1, c.getStats().getInMsgs());
            // InBytes should be incremented by length, even if the sub stats don't increase
            assertEquals(length, c.getStats().getInBytes());
            // handleSlowConsumer should have been called zero times
            verify(c, times(1)).handleSlowConsumer(eq(sub), any(Message.class));
            // sub.addMessage(msg) should have been called
            verify(mchMock, times(1)).add(any(Message.class));
            // the condition should not have been signaled
            verify(sub.pCond, times(0)).signal();
        }
    }

    @Test
    public void testProcessSlowConsumer()
            throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            final AtomicInteger ecbCount = new AtomicInteger(0);
            final CountDownLatch ecbLatch = new CountDownLatch(1);
            c.setExceptionHandler(new ExceptionHandler() {
                @Override
                public void onException(NATSException nex) {
                    assertTrue(nex.getCause() instanceof IOException);
                    assertEquals(Nats.ERR_SLOW_CONSUMER, nex.getCause().getMessage());
                    ecbCount.incrementAndGet();
                    ecbLatch.countDown();
                }
            });
            SubscriptionImpl sub = (SubscriptionImpl) c.subscribe("foo", new MessageHandler() {
                public void onMessage(Message msg) { /* NOOP */ }
            });

            c.processSlowConsumer(sub);
            ecbLatch.await(2, TimeUnit.SECONDS);
            assertEquals(1, ecbCount.get());
            assertTrue(sub.sc);

            // Now go a second time, making sure the handler wasn't invoked again
            c.processSlowConsumer(sub);
            assertEquals(1, ecbCount.get());

            sub.close();
            c.close();
        }
    }

    @Test
    public void testGetConnectedUrl() throws Exception {
        thrown.expect(NullPointerException.class);
        try (ConnectionImpl c = (ConnectionImpl) spy(newMockedConnection())) {
            assertEquals("nats://localhost:4222", c.getConnectedUrl());
            c.close();
            assertNull(c.getConnectedUrl());
        }

        try (ConnectionImpl c = (ConnectionImpl) spy(newMockedConnection())) {
            assertEquals("nats://localhost:4222", c.getConnectedUrl());
            doThrow(new NullPointerException()).when(c).getUrl();
            assertNull(c.getConnectedUrl());
        }
    }

    @Test
    public void testMakeTlsConn() throws Exception {
        // TODO put better checks in here. This is sloppy.
        ServerInfo info = new ServerInfo("asfasdfs", "127.0.0.1", 4224, "0.9.4", false, true,
                1024 * 1024, null);
        TcpConnectionFactory mcf = newMockedTcpConnectionFactory(info);
        Options opts = new Options.Builder(Nats.defaultOptions())
                .secure()
                .sslContext(SSLContext.getDefault())
                .factory(mcf).build();
        opts.url = "tls://localhost:4224";

        try (ConnectionImpl conn = (ConnectionImpl) spy(newMockedConnection(opts))) {
            assertEquals(conn.getOptions().getUrl(), conn.getConnectedUrl());
            assertTrue(conn.isConnected());
            // assertTrue(conn.getOptions().isSecure());
            // assertEquals(TLS_SCHEME, conn.getUrl().getScheme());
            // verify(conn, times(1)).createConn();
            // verify(conn, times(1)).processConnectInit();
            // verify(conn, times(1)).processExpectedInfo();
            // verify(conn, times(1)).checkForSecure();
            // verify(conn, times(1)).makeTlsConn();
        }
    }

    // @Test
    // public void testFlusherPreconditions()
    // throws Exception {
    // try (ConnectionImpl c = (ConnectionImpl) Mockito
    // .spy(new ConnectionImpl(Nats.defaultOptions()))) {
    // c.setFlushChannel(fchMock);
    // c.setOutputStream(bwMock);
    // TcpConnection tconn = mock(TcpConnection.class);
    // when(tconn.isConnected()).thenReturn(true);
    // c.setTcpConnection(tconn);
    // c.status = CONNECTED;
    //
    // c.conn = null;
    // c.flusher();
    // verify(fchMock, times(0)).take();
    //
    // c.setTcpConnection(tconn);
    // c.setOutputStream(null);
    // c.flusher();
    // verify(fchMock, times(0)).take();
    // }
    // }
    //
    // @Test
    // public void testFlusherChannelTakeInterrupted()
    // throws Exception {
    // try (ConnectionImpl c = (ConnectionImpl) Mockito
    // .spy(new ConnectionImpl(Nats.defaultOptions()))) {
    // c.setFlushChannel(fchMock);
    // c.setOutputStream(bwMock);
    // TcpConnection tconn = mock(TcpConnection.class);
    // when(tconn.isConnected()).thenReturn(true);
    // c.setTcpConnection(tconn);
    // c.status = CONNECTED;
    // doThrow(new InterruptedException("test")).when(fchMock).take();
    // c.flusher();
    // assertTrue(Thread.interrupted());
    // verify(bwMock, times(0)).flush();
    // }
    // }

    // @Test
    // public void testFlusherFlushError() throws Exception,
    // InterruptedException {
    // try (ConnectionImpl c = (ConnectionImpl) Mockito
    // .spy(new ConnectionImpl(Nats.defaultOptions()))) {
    // c.setFlushChannel(fchMock);
    // when(fchMock.take()).thenReturn(true).thenReturn(false);
    // c.setOutputStream(bwMock);
    // doThrow(new IOException("flush error")).when(bwMock).flush();
    //
    // TcpConnection tconn = mock(TcpConnection.class);
    // when(tconn.isConnected()).thenReturn(true);
    // c.setTcpConnection(tconn);
    // c.status = CONNECTED;
    // c.flusher();
    // verifier.verifyLogMsgEquals(Level.ERROR, "I/O exception encountered during flush");
    //
    // }
    // }

    @Test
    public void testGetServerInfo() throws Exception {
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            assertTrue(conn.isConnected());
            ServerInfo info = conn.getConnectedServerInfo();
            assertEquals("0.0.0.0", info.getHost());
            assertEquals("0.7.2", info.getVersion());
            assertEquals(4222, info.getPort());
            assertFalse(info.isAuthRequired());
            assertFalse(info.isTlsRequired());
            assertEquals(1048576, info.getMaxPayload());
        }
    }

    @Test
    public void testRequest() throws Exception {
        final String inbox = "_INBOX.DEADBEEF";
        // SyncSubscriptionImpl mockSub = mock(SyncSubscriptionImpl.class);
        Message replyMsg = new Message();
        replyMsg.setData("answer".getBytes());
        replyMsg.setSubject(inbox);
        when(syncSubMock.nextMessage(any(long.class), any(TimeUnit.class))).thenReturn(replyMsg);
        try (ConnectionImpl c = (ConnectionImpl) spy(newMockedConnection())) {
            doReturn(inbox).when(c).newInbox();
            doReturn(mchMock).when(c).createMsgChannel(anyInt());
            doReturn(syncSubMock).when(c).subscribe(inbox, null, null,
                    mchMock);
            Message msg = c.request("foo", null);
            verify(syncSubMock, times(1)).nextMessage(-1, TimeUnit.MILLISECONDS);
            assertEquals(replyMsg, msg);

            msg = c.request("foo", null, 1, TimeUnit.SECONDS);
            assertEquals(replyMsg, msg);

            msg = c.request("foo", null, 500);
            assertEquals(replyMsg, msg);
        }
    }

    @Test
    public void testRequestErrors() throws Exception {
        final String errMsg = "testRequestErrors()";
        thrown.expect(IOException.class);
        thrown.expectMessage(errMsg);
        try (ConnectionImpl nc = (ConnectionImpl) spy(newMockedConnection())) {
            when(nc.subscribeSync("foo", null)).thenReturn(syncSubMock);
            doThrow(new IOException(errMsg)).when(nc).publish(anyString(), anyString(),
                    any(byte[].class));

            nc.request("foo", "help".getBytes());
            verify(syncSubMock, times(0)).nextMessage(-1, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testConnectVerboseFails() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage("nats: expected '+OK', got 'PONG'");
        Options opts = new Options.Builder().verbose().build();
        try (ConnectionImpl conn = (ConnectionImpl) spy(newMockedConnection(opts))) {
            assertEquals(true, opts.isVerbose());
            assertEquals(true, conn.getOptions().isVerbose());
            assertTrue(conn.isConnected());
            BufferedReader br = conn.getTcpConnection().getBufferedReader();
        }
    }

    @Test
    public void testCreateConnBadTimeout() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_BAD_TIMEOUT);
        Options opts = new Options.Builder().timeout(-1).build();
        try (ConnectionImpl conn = (ConnectionImpl) spy(new ConnectionImpl(opts))) {
            conn.createConn();
        }
    }

    @Test
    public void testConnectVerbose() throws Exception {
        TcpConnectionFactory mcf = newMockedTcpConnectionFactory();
        TcpConnection tcpMock = newMockedTcpConnection();
        doReturn(tcpMock).when(mcf).createConnection();
        BufferedReader br = tcpMock.getBufferedReader();
        doReturn(defaultInfo, "+OK", "PONG").when(br).readLine();

        Options opts = new Options.Builder().factory(mcf).verbose().build();
        try (Connection c = newMockedConnection(opts)) {
            // Should be connected
        }
    }

    @Test
    public void testProcessPong() throws Exception {
        List<BlockingQueue<Boolean>> pongs = new ArrayList<>();
        BlockingQueue<Boolean> ch = new LinkedBlockingQueue<>();
        pongs.add(ch);
        try (ConnectionImpl conn = spy(new ConnectionImpl(defaultOptions()))) {
            conn.setPongs(pongs);
            doReturn(ch).when(conn).createBooleanChannel(1);
            conn.processPong();
            assertTrue(ch.take());

        }
    }

    @Test
    public void testSendConnectServerError() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage("nats: parser error");

        TcpConnectionFactory mcf = newMockedTcpConnectionFactory();
        TcpConnection tcpMock = newMockedTcpConnection();
        doReturn(tcpMock).when(mcf).createConnection();
        BufferedReader br = tcpMock.getBufferedReader();
        doReturn(defaultInfo, "-ERR 'Parser Error'").when(br).readLine();

        Options opts = new Options.Builder().factory(mcf).build();
        try (Connection c = newMockedConnection(opts)) {
            fail("Shouldn't have connected.");
        }
    }

    @Test
    public void testSendConnectFailsWhenPongIsNull() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage("nats: expected 'PONG', got ''");

        TcpConnectionFactory mcf = newMockedTcpConnectionFactory();
        TcpConnection tcpMock = newMockedTcpConnection();
        doReturn(tcpMock).when(mcf).createConnection();
        BufferedReader br = tcpMock.getBufferedReader();
        doReturn(defaultInfo, "").when(br).readLine();

        Options opts = new Options.Builder().factory(mcf).build();
        try (Connection c = newMockedConnection(opts)) {
            fail("Shouldn't have connected.");
        }
    }

    @Test
    public void testSendPingError() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            doThrow(new IOException("Mock OutputStream write exception")).when(bwMock).flush();
            c.setOutputStream(bwMock);
            c.sendPing(new LinkedBlockingQueue<Boolean>());
            assertTrue(c.getLastException() instanceof IOException);
            assertEquals("Mock OutputStream write exception", c.getLastException().getMessage());
        }
    }

    @Test
    public void testSendProto() throws Exception {
        byte[] pingProto = "PING\r\n".getBytes();
        int len = pingProto.length;
        try (ConnectionImpl conn = spy(new ConnectionImpl(defaultOptions()))) {
            conn.setOutputStream(bwMock);
            conn.sendProto(pingProto, len);
            verify(bwMock, atLeast(1)).write(pingProto, 0, len);
        }
    }

    @Test
    public void testSendSubscriptionMessage() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
            String subject = doReturn("foo").when(mockSub).getSubject();
            String queue = doReturn("bar").when(mockSub).getQueue();
            Long sid = doReturn(55L).when(mockSub).getSid();
            c.setOutputStream(bwMock);
            // Ensure bw write error is logged
            ConnectionAccessor.setState(c, CONNECTED);
            byte[] data = String.format(ConnectionImpl.SUB_PROTO, subject, queue, sid).getBytes();
            doThrow(new IOException("test")).when(bwMock).write(any(byte[].class));
            c.sendSubscriptionMessage(mockSub);
            verify(bwMock).write(any(byte[].class));
            verifier.verifyLogMsgEquals(Level.WARN,
                    "nats: I/O exception while sending subscription message");
        }
    }

    @Test
    public void testGetSetInputStream() throws Exception {
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            conn.setInputStream(brMock);
            assertEquals(brMock, conn.getInputStream());
        }
    }

    @Test
    public void testGetSetHandlers() throws Exception {
        ClosedCallback ccb = mock(ClosedCallback.class);
        DisconnectedCallback dcb = mock(DisconnectedCallback.class);
        ReconnectedCallback rcb = mock(ReconnectedCallback.class);
        ExceptionHandler eh = mock(ExceptionHandler.class);
        try (ConnectionImpl c = new ConnectionImpl(defaultOptions())) {
            c.setup();
            assertNull(c.getClosedCallback());
            assertNull(c.getReconnectedCallback());
            assertNull(c.getDisconnectedCallback());
            assertNull(c.getExceptionHandler());
            c.setClosedCallback(ccb);
            assertEquals(ccb, c.getClosedCallback());
            c.setDisconnectedCallback(dcb);
            assertEquals(dcb, c.getDisconnectedCallback());
            c.setReconnectedCallback(rcb);
            assertEquals(rcb, c.getReconnectedCallback());
            c.setExceptionHandler(eh);
            assertEquals(eh, c.getExceptionHandler());
        }
    }

    @Test
    public void testNormalizeErr() {
        final String errString = "-ERR 'Authorization Violation'";
        ByteBuffer error = ByteBuffer.allocate(1024);
        error.put(errString.getBytes());
        error.flip();

        String str = ConnectionImpl.normalizeErr(error);
        assertEquals("authorization violation", str);
    }

    @Test
    public void testPingTimerTask() throws Exception {
        try (ConnectionImpl connection = (ConnectionImpl) spy(newMockedConnection())) {
            System.err.printf("Connection state is %s\n", connection.getState());
            assertEquals(CONNECTED, connection.getState());
            assertTrue(connection.isConnected());
            assertTrue(connection.connected());
            assertTrue(connection.getActualPingsOutstanding() + 1 < connection.getOptions().getMaxPingsOut());
            SynchronousExecutorService sexec = new SynchronousExecutorService();
            sexec.submit(connection.new PingTimerTask());
            verify(connection, times(1)).sendPing(null);
        }
    }

    @Test
    public void testPingTimerTaskFailsFastIfNotConnected() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) spy(newMockedConnection())) {
            doReturn(false).when(c).connected();
            SynchronousExecutorService sexec = new SynchronousExecutorService();
            sexec.submit(c.new PingTimerTask());
            verify(c, times(0)).sendPing(null);
            doReturn(true).when(c).connected();
        }
    }

    @Test
    public void testResetPingTimer() throws Exception {
        try (ConnectionImpl nc = (ConnectionImpl) spy(newMockedConnection())) {
            ScheduledFuture ptmrMock = mock(ScheduledFuture.class);
            //noinspection unchecked
            when(nc.createPingTimer()).thenReturn(ptmrMock);

            // Test for ptmr already exists
            nc.setPingTimer(ptmrMock);
            nc.resetPingTimer();
            // verify old was cancelled
            verify(ptmrMock, times(1)).cancel(true);
            // verify new was created
            verify(nc, times(1)).createPingTimer();
            assertEquals(ptmrMock, nc.getPingTimer());

            // Test for ping interval <= 0
            nc.setPingTimer(null);
            nc.setOptions(new Options.Builder().pingInterval(0).build());
            nc.resetPingTimer();
            // Verify that no ping timer was created, since ping interval was 0
            assertNull(nc.getPingTimer());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPingTimerTaskMaxPingsOutExceeded() throws Exception {
        Options opts = new Options.Builder().noReconnect().maxPingsOut(4).build();
        try (ConnectionImpl nc = (ConnectionImpl) spy(newMockedConnection(opts))) {
            nc.setActualPingsOutstanding(5);
            SynchronousExecutorService sexec = new SynchronousExecutorService();
            sexec.execute(nc.new PingTimerTask());
            verify(nc, times(1)).processOpError(any(IOException.class));
            verify(nc, times(0)).sendPing(any(BlockingQueue.class));
            assertTrue(nc.getLastException() instanceof IOException);
            assertEquals(ERR_STALE_CONNECTION, nc.getLastException().getMessage());
            assertTrue(nc.isClosed());
        }
    }

    @Test
    public void testThreadsExitOnClose() throws Exception {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
//            setLogLevel(Level.TRACE);
            c.close();
            ExecutorService exec = ConnectionAccessor.getExec(c);
            assertTrue(exec.isShutdown());
            assertTrue(exec.isTerminated());
            assertTrue(String.format("Expected %s to be terminated.", ConnectionImpl.EXEC_NAME),
                    exec.awaitTermination(2, TimeUnit.SECONDS));

            ExecutorService subexec = ConnectionAccessor.getSubExec(c);
            assertTrue(subexec.isShutdown());
            assertTrue(subexec.isTerminated());
            assertTrue(String.format("Expected %s to be terminated.", ConnectionImpl.SUB_EXEC_NAME),
                    subexec.awaitTermination(2, TimeUnit.SECONDS));

            ExecutorService cbexec = ConnectionAccessor.getCbExec(c);
            assertTrue(cbexec.isShutdown());
            assertTrue(cbexec.isTerminated());
            assertTrue(String.format("Expected %s to be terminated.", ConnectionImpl.CB_EXEC_NAME),
                    cbexec.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testTlsMismatchClient() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_SECURE_CONN_WANTED);

        Options opts = new Options.Builder().secure().build();
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection(opts)) {
            try {
                fail("Shouldn't have connected.");
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                assertTrue(conn.getOptions().isSecure());
                verify(conn, times(1)).checkForSecure();
            }
        }
    }

    @Test
    public void testTlsMismatchServer() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_SECURE_CONN_REQUIRED);

        ServerInfo info = ServerInfo.createFromWire(defaultInfo);
        info.setTlsRequired(true);
        TcpConnectionFactory mcf = newMockedTcpConnectionFactory(info);

        Options opts = new Options.Builder().factory(mcf).build();
        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection(opts)) {
            assertEquals(info.toString(), conn.getConnectedServerInfo().toString());
            assertEquals(mcf, conn.getTcpConnectionFactory());
            // System.err.println("Server info was " + conn.getConnectedServerInfo());
            fail("Shouldn't have connected.");
        }
    }

    @Test
    public void testToString() throws Exception {
        assertNotNull(new ConnectionImpl(defaultOptions()).toString());
    }

    @Test
    public void testUnsubscribe() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_CONNECTION_CLOSED);

        try (ConnectionImpl nc = (ConnectionImpl) newMockedConnection()) {
            SyncSubscriptionImpl sub = (SyncSubscriptionImpl) nc.subscribeSync("foo");
            long sid = sub.getSid();
            assertNotNull("Sub should have been present", nc.getSubs().get(sid));
            sub.unsubscribe();
            assertNull("Sub should have been removed", nc.getSubs().get(sid));
            nc.close();
            assertTrue(nc.isClosed());
            nc.unsubscribe(sub, 0);
        }
    }


    @Test
    public void testUnsubscribeAlreadyUnsubscribed() throws Exception {
        try (ConnectionImpl nc = (ConnectionImpl) spy(newMockedConnection())) {
            nc.setSubs(subsMock);
            SubscriptionImpl sub = mock(AsyncSubscriptionImpl.class);
            nc.unsubscribe(sub, 100);
            verify(sub, times(0)).setMax(any(long.class));
            verify(nc, times(0)).removeSub(eq(sub));
        }
    }

    @Test
    public void testUnsubscribeWithMax() throws Exception {
        try (ConnectionImpl nc = (ConnectionImpl) spy(newMockedConnection())) {
            nc.setSubs(subsMock);
            SubscriptionImpl sub = mock(AsyncSubscriptionImpl.class);
            when(sub.getSid()).thenReturn(14L);
            when(subsMock.get(eq(14L))).thenReturn(sub);

            nc.unsubscribe(sub, 100);
            verify(sub, times(1)).setMax(eq(100L));
            verify(nc, times(0)).removeSub(eq(sub));
        }
    }

    @Test
    public void testGetServers() throws Exception {
        Options opts = new Options.Builder().dontRandomize().build();
        ConnectionImpl conn = new ConnectionImpl(opts);
        conn.setupServerPool();

        // Check the default url
        assertArrayEquals(new String[] {Nats.DEFAULT_URL}, conn.getServers());
        assertEquals("Expected no discovered servers", 0, conn.getDiscoveredServers().length);

        // Add a new URL
        Parser parser = ConnectionAccessor.getParser(conn);
        parser.parse("INFO {\"connect_urls\":[\"localhost:5222\"]}\r\n".getBytes());

        // Server list should now contain both the default and the new url.
        String[] expected = {"nats://localhost:4222", "nats://localhost:5222"};
        assertArrayEquals(expected, conn.getServers());

        // Discovered servers should only contain the new url.
        String[] actual = conn.getDiscoveredServers();
        assertEquals(1, actual.length);
        assertEquals("nats://localhost:5222", actual[0]);

        // verify user credentials are stripped out.
        opts.servers = Nats.processUrlString("nats://user:pass@localhost:4333, " +
                "nats://token@localhost:4444");
        conn = new ConnectionImpl(opts);
        conn.setupServerPool();

        expected = new String[] {"nats://localhost:4333", "nats://localhost:4444"};
        assertArrayEquals(expected, conn.getServers());
    }

    @Test
    public void testHandleSlowConsumer() throws Exception {
        MessageHandler mcb = new MessageHandler() {
            public void onMessage(Message msg) {

            }

        };
        try (ConnectionImpl nc = (ConnectionImpl) spy(newMockedConnection())) {
            AsyncSubscriptionImpl sub = new AsyncSubscriptionImpl(nc, "foo", "bar", mcb);
            Message msg = new Message("foo", "bar", "Hello World".getBytes());
            sub.pBytes += msg.getData().length;
            sub.pMsgs = 1;
            nc.handleSlowConsumer(sub, msg);
            assertEquals(1, sub.dropped);
            assertEquals(0, sub.pMsgs);
            assertEquals(0, sub.pBytes);

            msg.setData(null);
            sub.pMsgs = 1;
            nc.handleSlowConsumer(sub, msg);
            assertEquals(2, sub.getDropped());
            assertEquals(0, sub.pMsgs);
            assertEquals(0, sub.pBytes);
        }

    }

    @Test
    public void testIsConnected() throws Exception {
        try (ConnectionImpl nc = (ConnectionImpl) spy(newMockedConnection())) {
            assertEquals(CONNECTED, nc.getState());
        }
    }

}
