/**
 * 
 */
package io.nats.client;

import static io.nats.client.Constants.ERR_BAD_SUBJECT;
import static io.nats.client.Constants.ERR_BAD_TIMEOUT;
import static io.nats.client.Constants.ERR_CONNECTION_CLOSED;
import static io.nats.client.Constants.ERR_CONNECTION_READ;
import static io.nats.client.Constants.ERR_MAX_PAYLOAD;
import static io.nats.client.Constants.ERR_NO_SERVERS;
import static io.nats.client.Constants.ERR_PROTOCOL;
import static io.nats.client.Constants.ERR_TIMEOUT;
import static io.nats.client.UnitTestUtilities.newMockedConnection;
import static io.nats.client.UnitTestUtilities.setLogLevel;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.client.ConnectionImpl.Control;
import io.nats.client.Constants.ConnState;

import ch.qos.logback.classic.Level;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLContext;

@Category(UnitTest.class)
public class ConnectionImplTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(ConnectionImplTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @Mock
    private Channel<Boolean> fchMock;

    @Mock
    private Channel<Boolean> pongsMock;

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

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        verifier.setup();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        verifier.teardown();
        setLogLevel(Level.INFO);
    }

    @SuppressWarnings("resource")
    @Test
    public void testConnectionImpl() {
        new ConnectionImpl();
    }

    @Test
    public void testConnectProto() throws IOException, TimeoutException {
        Options opts = new Options();
        opts.setUrl("nats://derek:foo@localhost:4222");
        try (ConnectionImpl conn = (ConnectionImpl) new ConnectionImpl(opts)) {
            assertNotNull(conn.getUrl().getUserInfo());
            conn.setOutputStream(bwMock);
            String proto = conn.connectProto();
            ClientConnectInfo connectInfo = ClientConnectInfo.createFromWire(proto);
            assertEquals("derek", connectInfo.getUser());
            assertEquals("foo", connectInfo.getPass());
        }

        opts.setUrl("nats://thisismytoken@localhost:4222");
        try (ConnectionImpl conn = (ConnectionImpl) new ConnectionImpl(opts)) {
            assertNotNull(conn.getUrl().getUserInfo());
            conn.setOutputStream(bwMock);
            String proto = conn.connectProto();
            ClientConnectInfo connectInfo = ClientConnectInfo.createFromWire(proto);
            assertEquals("thisismytoken", connectInfo.getToken());
        }

    }

    @Test
    public void testGetPropertiesInputStreamFailures() throws IOException {
        ConnectionImpl conn = new ConnectionImpl(new Options());
        conn.setOutputStream(bwMock);
        InputStream is = mock(InputStream.class);
        doThrow(new IOException("fake I/O exception")).when(is).read(any(byte[].class));

        setLogLevel(ch.qos.logback.classic.Level.DEBUG);
        conn.getProperties(is);
        verifier.verifyLogMsgEquals(Level.DEBUG, "nats: error loading properties from InputStream");
        setLogLevel(ch.qos.logback.classic.Level.INFO);

        Properties props = conn.getProperties((InputStream) null);
        assertNull(props);
        conn.close();
    }

    @Test
    public void testIsReconnecting() throws IOException, TimeoutException {
        try (ConnectionImpl conn = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            assertFalse(conn.isReconnecting());
            conn.status = ConnState.RECONNECTING;
            assertTrue(conn.isReconnecting());
            conn.status = ConnState.CONNECTED;
        }
    }

    @Test
    public void testSelectNextServerNoServers() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(ERR_NO_SERVERS);

        try (ConnectionImpl conn = (ConnectionImpl) newMockedConnection()) {
            List<ConnectionImpl.Srv> pool = conn.getServerPool();
            pool.clear();
            ConnectionImpl.Srv srv = conn.selectNextServer();
        }
    }

    @Test
    public void testSelectNextServer() throws Exception {
        Options opts = new Options();
        opts.setMaxReconnect(-1);
        opts.setNoRandomize(true);
        opts.setServers(new String[] { "nats://localhost:5222", "nats://localhost:6222" });
        ConnectionImpl conn = new ConnectionImpl(opts);
        List<ConnectionImpl.Srv> pool = conn.getServerPool();
        assertEquals("nats://localhost:5222", conn.currentServer().url.toString());

        ConnectionImpl.Srv srv = conn.selectNextServer();
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

        conn.getOptions().setMaxReconnect(1);
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
    public void testFlushReconnectPendingItems() throws IOException, TimeoutException {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            ByteArrayOutputStream baos = mock(ByteArrayOutputStream.class);
            when(baos.size()).thenReturn(c.pingProtoBytesLen);
            when(baos.toByteArray()).thenReturn(c.pingProtoBytes);

            assertNull(c.getPending());

            // Test successful flush
            c.setPending(baos);
            c.setOutputStream(bwMock);
            c.flushReconnectPendingItems();
            verify(baos, times(1)).toByteArray();
            verify(bwMock, times(1)).write(eq(c.pingProtoBytes), eq(0), eq(c.pingProtoBytesLen));

            // Test with PING pending
            doThrow(new IOException("IOException from testFlushReconnectPendingItems")).when(bwMock)
                    .flush();

            c.setPending(baos);
            c.setOutputStream(bwMock);
            c.flushReconnectPendingItems();
            verifier.verifyLogMsgEquals(Level.ERROR, "Error flushing pending items");
            verify(baos, times(2)).toByteArray();
            verify(bwMock, times(2)).write(eq(c.pingProtoBytes), eq(0), eq(c.pingProtoBytesLen));
        }
    }

    @Test
    public void testReadOp() throws IOException, TimeoutException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            TCPConnection connMock = mock(TCPConnection.class);
            c.setTcpConnection(connMock);
            BufferedReader brMock = mock(BufferedReader.class);
            when(connMock.getBufferedReader()).thenReturn(brMock);
            when(brMock.readLine()).thenReturn("PING\r\n");
            Control pingLine = c.readOp();
            assertEquals(pingLine.op, "PING");
        }
    }

    @Test
    public void testReadOpNull() {
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        mcf.setCloseStream(true);
        try (ConnectionImpl c = new ConnectionFactory().createConnection(mcf)) {
            fail("Shouldn't have connected.");
        } catch (IOException | TimeoutException e) {
            assertTrue(e instanceof IOException);
            assertEquals(ERR_CONNECTION_READ, e.getMessage());
        }
    }

    @Test
    public void testRemoveFlushEntry() {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            ArrayList<Channel<Boolean>> pongs = new ArrayList<Channel<Boolean>>();
            c.setPongs(pongs);
            assertEquals(pongs, c.getPongs());
            // Basic case
            Channel<Boolean> testChan = new Channel<Boolean>();
            testChan.add(true);
            pongs.add(testChan);
            assertTrue("Failed to find chan", c.removeFlushEntry(testChan));

            Channel<Boolean> testChan2 = new Channel<Boolean>();
            testChan2.add(false);
            pongs.add(testChan);
            assertFalse("Should not have found chan", c.removeFlushEntry(testChan2));

            pongs.clear();
            assertFalse("Should have returned false", c.removeFlushEntry(testChan));

        } catch (IOException | TimeoutException e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testDoReconnect() throws IOException, TimeoutException, InterruptedException {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            final CountDownLatch dcbLatch = new CountDownLatch(1);
            final AtomicInteger dcbCount = new AtomicInteger(0);
            c.setDisconnectedCallback(new DisconnectedCallback() {
                public void onDisconnect(ConnectionEvent event) {
                    dcbLatch.countDown();
                    dcbCount.incrementAndGet();
                }
            });
            c.opts.setReconnectAllowed(true);
            c.opts.setReconnectWait(1);
            c.doReconnect();
            assertTrue("DisconnectedCallback not triggered", dcbLatch.await(2, TimeUnit.SECONDS));
            assertEquals(1, dcbCount.get());

            c.opts.setDisconnectedCallback(null);
            c.doReconnect();
            assertEquals(1, dcbCount.get());
        }
    }

    // @Test
    // public void testDoReconnectFlushFails()
    // throws IOException, TimeoutException, InterruptedException {
    // final ByteArrayOutputStream mockOut =
    // mock(ByteArrayOutputStream.class, withSettings().verboseLogging());
    // try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
    // final CountDownLatch rcbLatch = new CountDownLatch(1);
    // final AtomicInteger rcbCount = new AtomicInteger(0);
    // c.setReconnectedCallback(new ReconnectedCallback() {
    // public void onReconnect(ConnectionEvent event) {
    // rcbCount.incrementAndGet();
    // rcbLatch.countDown();
    // }
    // });
    //
    // c.opts.setReconnectAllowed(true);
    // c.opts.setMaxReconnect(1);
    // c.opts.setReconnectWait(1000);
    //
    // doAnswer(new Answer<Void>() {
    // @Override
    // public Void answer(InvocationOnMock invocation) throws Throwable {
    // logger.error("HELLO in doAnswer");
    // c.setOutputStream(mockOut);
    // return null;
    // }
    // }).when(c).flushReconnectPendingItems();
    //
    // doThrow(new IOException("flush error")).when(mockOut).flush();
    // // when(c.getOutputStream()).thenReturn(mockOut);
    // setLogLevel(Level.TRACE);
    // TCPConnectionMock mockServer = (TCPConnectionMock) c.getTcpConnection();
    // c.setPending(mockOut);
    // mockServer.bounce();
    // sleep(100);
    // verify(c, times(1)).getOutputStream();
    //
    // // c.doReconnect();
    // // verifier.verifyLogMsgEquals(Level.DEBUG, "Error flushing output stream");
    //
    // assertTrue("DisconnectedCallback not triggered", rcbLatch.await(5, TimeUnit.SECONDS));
    // assertEquals(1, rcbCount.get());
    // verify(mockOut, times(1)).flush();
    // }
    // }

    @Test
    public void testDoReconnectNoServers()
            throws IOException, TimeoutException, InterruptedException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            c.opts.setReconnectAllowed(true);
            c.opts.setReconnectWait(1);
            doThrow(new IOException(ERR_NO_SERVERS)).when(c).selectNextServer();
            c.doReconnect();
            assertTrue(c.getLastException() instanceof IOException);
            assertEquals(ERR_NO_SERVERS, c.getLastException().getMessage());
        }
    }

    @Test
    public void testDoReconnectCreateConnFailed()
            throws IOException, TimeoutException, InterruptedException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            c.opts.setReconnectAllowed(true);
            c.opts.setReconnectWait(1);
            doThrow(new IOException(ERR_NO_SERVERS)).when(c).createConn();
            c.doReconnect();
            assertTrue(c.getLastException() instanceof IOException);
            assertEquals(ERR_NO_SERVERS, c.getLastException().getMessage());
        }
    }

    @Test
    public void testDoReconnectProcessConnInitFailed()
            throws IOException, TimeoutException, InterruptedException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            c.opts.setReconnectAllowed(true);
            c.opts.setReconnectWait(1);
            c.opts.setMaxReconnect(1);

            doThrow(new IOException(ERR_PROTOCOL + ", INFO not received")).when(c)
                    .processExpectedInfo();
            c.doReconnect();
            verifier.verifyLogMsgMatches(Level.WARN, "doReconnect: processConnectInit FAILED.+$");
        }
    }

    @Test
    public void testDoReconnectConnClosed()
            throws IOException, TimeoutException, InterruptedException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            c.opts.setReconnectAllowed(true);
            c.opts.setReconnectWait(1);
            c.opts.setMaxReconnect(1);

            when(c.isClosed()).thenReturn(true);
            c.doReconnect();
            assertEquals(ERR_NO_SERVERS, c.getLastException().getMessage());
        }
    }

    @Test
    public void testDoReconnectCallsReconnectedCb()
            throws IOException, TimeoutException, InterruptedException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            final AtomicInteger rcbCount = new AtomicInteger(0);
            final CountDownLatch rcbLatch = new CountDownLatch(1);

            c.opts.setReconnectAllowed(true);
            c.opts.setReconnectWait(1);
            c.opts.setMaxReconnect(1);
            c.setReconnectedCallback(new ReconnectedCallback() {
                public void onReconnect(ConnectionEvent event) {
                    rcbCount.incrementAndGet();
                    rcbLatch.countDown();
                }
            });
            c.doReconnect();
            assertTrue(rcbLatch.await(5, TimeUnit.SECONDS));
            assertEquals(1, rcbCount.get());
        }
    }

    @Test
    public void testResendSubscriptions() throws IOException, TimeoutException {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            AsyncSubscriptionImpl sub =
                    (AsyncSubscriptionImpl) c.subscribe("foo", new MessageHandler() {
                        public void onMessage(Message msg) {
                            System.err.println("got msg: " + msg);
                        }
                    });
            sub.setMax(122);
            assertEquals(122, sub.max);
            sub.delivered.set(122);
            assertEquals(122, sub.delivered.get());
            c.resendSubscriptions();
            c.getOutputStream().flush();
            sleep(100);
            String str = String.format("UNSUB %d", sub.getSid());
            TCPConnectionMock mock = (TCPConnectionMock) c.getTcpConnection();
            assertEquals(str, mock.getBuffer());

            SyncSubscriptionImpl syncSub = (SyncSubscriptionImpl) c.subscribeSync("foo");
            syncSub.setMax(10);
            syncSub.delivered.set(8);
            long adjustedMax = (syncSub.getMax() - syncSub.delivered.get());
            assertEquals(2, adjustedMax);
            c.resendSubscriptions();
            c.getOutputStream().flush();
            sleep(100);
            str = String.format("UNSUB %d %d", syncSub.getSid(), adjustedMax);
            assertEquals(str, mock.getBuffer());
        }
    }

    @Test
    public void testPublishIoError() throws IOException, TimeoutException {
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
    public void testPublishBadSubject() throws IOException, TimeoutException {
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
    public void testPublishWithReply() throws IOException, TimeoutException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            c.publish("foo", "bar", null);
            verify(c, times(1))._publish(eq("foo".getBytes()), eq("bar".getBytes()),
                    eq((byte[]) null));
        }
    }

    @Test
    public void testErrOnMaxPayloadLimit() throws IOException, TimeoutException {
        long expectedMaxPayload = 10;
        String serverInfo =
                "INFO {\"server_id\":\"foobar\",\"version\":\"0.6.6\",\"go\":\"go1.5.1\",\"host\":\"%s\",\"port\":%d,\"auth_required\":false,\"ssl_required\":false,\"max_payload\":%d}\r\n";

        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        String infoString = (String.format(serverInfo, "mockserver", 2222, expectedMaxPayload));
        // System.err.println(infoString);
        mcf.setServerInfoString(infoString);
        ConnectionFactory cf = new ConnectionFactory();
        try (Connection c = cf.createConnection(mcf)) {
            // Make sure we parsed max payload correctly
            assertEquals(c.getMaxPayload(), expectedMaxPayload);

            // Check for correct exception
            boolean exThrown = false;
            try {
                c.publish("hello", "hello world".getBytes());
            } catch (IllegalArgumentException e) {
                assertEquals(ERR_MAX_PAYLOAD, e.getMessage());
                exThrown = true;
            } finally {
                assertTrue("Should have generated a IllegalArgumentException.", exThrown);
            }

            // Check for success on less than maxPayload

        }
    }

    @Test
    public void testPublishClosedConnection() throws IOException, TimeoutException {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(Constants.ERR_CONNECTION_CLOSED);
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            c.close();
            c.publish("foo", null);
        }
    }

    @Test
    public void testPublishReconnectingFlushError() throws IOException, TimeoutException {
        OutputStream os = mock(OutputStream.class);
        doThrow(new IOException("test")).when(os).flush();
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            c.setOutputStream(os);
            c.setPending(mock(ByteArrayOutputStream.class));
            c.status = ConnState.RECONNECTING;
            c.publish("foo", null);
            verifier.verifyLogMsgEquals(Level.ERROR, "I/O exception during flush");
        }
    }

    @Test
    public void testPublishReconnectingPendingBufferTooLarge()
            throws IOException, TimeoutException {
        thrown.expect(IOException.class);
        thrown.expectMessage(Constants.ERR_RECONNECT_BUF_EXCEEDED);

        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            c.setOutputStream(bwMock);
            c.setPending(pendingMock);
            when(pendingMock.size()).thenReturn(c.opts.getReconnectBufSize() + 10);
            c.status = ConnState.RECONNECTING;
            c.publish("foo", null);
            verifier.verifyLogMsgEquals(Level.ERROR, "I/O exception during flush");
        }
    }

    @Test
    public void testPublishBufferOverflow() throws IOException, TimeoutException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
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
    public void testGetPropertiesStringFailure() throws IOException, TimeoutException {
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
    public void testCreateConnFailure() {
        boolean exThrown = false;
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        mcf.setOpenFailure(true);
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection(mcf)) {
            fail("Should not have connected");
        } catch (IOException | TimeoutException e) {
            exThrown = true;
            String name = e.getClass().getSimpleName();
            assertTrue("Expected IOException, but got " + name, e instanceof IOException);
            assertEquals(ERR_NO_SERVERS, e.getMessage());
        }
        assertTrue("Should have thrown exception.", exThrown);
    }

    @Test
    public void testClosedCallback() throws IOException, TimeoutException, InterruptedException {
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
    public void testClosedConnections() throws IOException, TimeoutException {
        Connection c = newMockedConnection();
        SyncSubscription s = c.subscribeSync("foo");

        c.close();
        assertTrue(c.isClosed());

        // While we can annotate all the exceptions in the test framework,
        // just do it manually.

        boolean exThrown = false;

        try {
            c.publish("foo", null);
        } catch (Exception e) {
            assertTrue("Expected IllegalStateException, got " + e.getClass().getSimpleName(),
                    e instanceof IllegalStateException);
            assertEquals(ERR_CONNECTION_CLOSED, e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Didn't throw an exception", exThrown);
        }

        exThrown = false;
        assertTrue(c.isClosed());
        try {
            c.publish(new Message("foo", null, null));
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
            c.subscribeAsync("foo");
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
            c.subscribeSync("foo");
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
            c.subscribeAsync("foo", "bar");
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
            c.subscribeSync("foo", "bar");
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
            c.request("foo", null);
            assertTrue(c.isClosed());
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
            s.nextMessage();
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
            s.nextMessage(100);
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
            s.unsubscribe();
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
            s.autoUnsubscribe(1);
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
    public void testCloseNullTcpConn() throws IOException, TimeoutException, InterruptedException {
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
            assertTrue(dcbLatch.await(2, TimeUnit.SECONDS));
        }
    }

    // @Test
    // public void testCloseAwaitingCallbackExecShutdown()
    // throws IOException, TimeoutException, InterruptedException {
    // ConnectionImpl conn = (ConnectionImpl) newMockedConnection();
    // conn.cbexec = cbExecMock;
    // when(cbExecMock.awaitTermination(any(long.class), any(TimeUnit.class))).thenReturn(false)
    // .thenReturn(true);
    // setLogLevel(Level.DEBUG);
    //
    // conn.close();
    //
    // verifier.verifyLogMsgEquals(Level.DEBUG, "Awaiting completion of threads.");
    // setLogLevel(Level.INFO);
    // }

    // @Test
    // public void testCloseCallbackExecTermInterrupted()
    // throws IOException, TimeoutException, InterruptedException {
    // ConnectionImpl conn = (ConnectionImpl) newMockedConnection();
    // conn.cbexec = cbExecMock;
    // doThrow(new InterruptedException("test interrupt")).when(cbExecMock)
    // .awaitTermination(any(long.class), any(TimeUnit.class));
    //
    // setLogLevel(Level.DEBUG);
    // conn.close();
    //
    // verifier.verifyLogMsgMatches(Level.DEBUG, "Interrupted waiting to shutdown cbexec.*$");
    // setLogLevel(Level.INFO);
    // }

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
        thrown.expect(TimeoutException.class);
        thrown.expectMessage(ERR_TIMEOUT);
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            TCPConnectionMock mock = (TCPConnectionMock) c.getTcpConnection();
            mock.setNoPongs(true);
            c.flush(500);
        }
    }

    @Test
    public void testFlushClosedFailure() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_CONNECTION_CLOSED);
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            c.close();
            c.flush(500);
        }
    }

    @Test
    public void testFlushFalsePongFailure() throws Exception {
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage(ERR_CONNECTION_CLOSED);
        @SuppressWarnings("unchecked")
        Channel<Boolean> ch = (Channel<Boolean>) mock(Channel.class);
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            when(c.createPongChannel(1)).thenReturn(ch);
            when(ch.poll()).thenReturn(false);
            c.flush(500);
        }
    }

    @Test
    public void testDeliverMsgsConnClosed() throws IOException, TimeoutException {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            Channel<Message> ch = new Channel<Message>();
            Message msg = new Message();
            ch.add(msg);
            assertEquals(1, ch.getCount());
            c.close();
            c.deliverMsgs(ch);
            assertEquals(1, ch.getCount());
        }
    }

    @Test
    public void testDeliverMsgsSubProcessFail() throws IOException, TimeoutException {
        final String subj = "foo";
        final String plString = "Hello there!";
        final byte[] payload = plString.getBytes();
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            final SyncSubscriptionImpl sub = mock(SyncSubscriptionImpl.class);
            when(sub.getSid()).thenReturn(14L);
            when(sub.processMsg(any(Message.class))).thenReturn(false);
            when(sub.getLock()).thenReturn(mock(ReentrantLock.class));

            @SuppressWarnings("unchecked")
            Channel<Message> ch = (Channel<Message>) mock(Channel.class);
            when(ch.get()).thenReturn(new Message(payload, payload.length, subj, null, sub))
                    .thenReturn(null);

            try {
                c.deliverMsgs(ch);
            } catch (Error e) {
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testProcessExpectedInfoReadOpFailure() throws IOException, TimeoutException {
        try (ConnectionImpl conn = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            TCPConnection connMock = mock(TCPConnection.class);
            BufferedReader brMock = mock(BufferedReader.class);
            when(connMock.getBufferedReader()).thenReturn(brMock);
            conn.setTcpConnection(connMock);
            doThrow(new IOException("flush error")).when(brMock).readLine();
            conn.processExpectedInfo();
            verify(conn, times(1)).processOpError(any(IOException.class));
        }
    }

    @Test
    public void testProcessSlowConsumer()
            throws IOException, TimeoutException, InterruptedException {
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            final AtomicInteger ecbCount = new AtomicInteger(0);
            final CountDownLatch ecbLatch = new CountDownLatch(1);
            c.setExceptionHandler(new ExceptionHandler() {
                @Override
                public void onException(NATSException nex) {
                    assertTrue(nex.getCause() instanceof IOException);
                    assertEquals(Constants.ERR_SLOW_CONSUMER, nex.getCause().getMessage());
                    ecbCount.incrementAndGet();
                    ecbLatch.countDown();
                }
            });
            SubscriptionImpl sub = (SubscriptionImpl) c.subscribe("foo", new MessageHandler() {
                @Override
                public void onMessage(Message msg) { /* NOOP */ }
            });
            c.processSlowConsumer(sub);
            ecbLatch.await(2, TimeUnit.SECONDS);
            assertEquals(1, ecbCount.get());
            assertTrue(sub.sc);

            // Now go a second time, making sure the handler wasn't invoked again
            c.processSlowConsumer(sub);
            assertEquals(1, ecbCount.get());
        }
    }

    @Test
    public void testGetConnectedUrl() throws IOException, TimeoutException, NullPointerException {
        thrown.expect(NullPointerException.class);
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            assertEquals("nats://localhost:4222", c.getConnectedUrl());
            c.close();
            assertNull(c.getConnectedUrl());
        }

        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            assertEquals("nats://localhost:4222", c.getConnectedUrl());
            doThrow(new NullPointerException()).when(c).getUrl();
            assertNull(c.getConnectedUrl());
        }
    }

    @Test
    public void testMakeTlsConn() throws IOException, TimeoutException, NoSuchAlgorithmException {
        // TODO put better checks in here. This is sloppy.
        Options opts = new ConnectionFactory().options();
        opts.setSecure(true);
        opts.setSSLContext(SSLContext.getDefault());
        opts.setUrl("tls://localhost:4224");
        TCPConnectionFactoryMock mcf = new TCPConnectionFactoryMock();
        ServerInfo info = new ServerInfo("asfasdfs", "127.0.0.1", 4224, "0.9.4", false, true,
                1024 * 1024, null);
        mcf.setServerInfoString(info.toString());
        ConnectionImpl conn = new ConnectionImpl(opts, mcf);
        try {
            conn.connect();
            conn.makeTLSConn();
        } catch (Exception e) {
            System.err.println(conn.getConnectedServerInfo());
            e.printStackTrace();
        } finally {
            conn.close();
        }
    }

    @Test
    public void testFlusherChannelGetFalse() throws IOException, TimeoutException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito
                .spy(new ConnectionImpl(new ConnectionFactory().options()))) {
            c.setFlushChannel(fchMock);
            c.setOutputStream(bwMock);
            TCPConnection tconn = mock(TCPConnection.class);
            when(tconn.isConnected()).thenReturn(true);
            c.setTcpConnection(tconn);
            c.status = ConnState.CONNECTED;
            when(fchMock.get()).thenReturn(false);
            c.flusher();
            verify(bwMock, times(0)).flush();
        }
    }

    @Test
    public void testFlusherFlushError() throws IOException, TimeoutException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito
                .spy(new ConnectionImpl(new ConnectionFactory().options()))) {
            c.setFlushChannel(fchMock);
            when(fchMock.get()).thenReturn(true).thenReturn(false);
            c.setOutputStream(bwMock);
            doThrow(new IOException("flush error")).when(bwMock).flush();

            TCPConnection tconn = mock(TCPConnection.class);
            when(tconn.isConnected()).thenReturn(true);
            c.setTcpConnection(tconn);
            c.status = ConnState.CONNECTED;
            c.flusher();
            verifier.verifyLogMsgEquals(Level.ERROR, "I/O eception encountered during flush");

        }
    }

    @Test
    public void testRequest() throws IOException, TimeoutException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Timeout must be greater that 0.");

        SyncSubscription mockSub = mock(SyncSubscription.class);
        Message replyMsg = new Message();
        replyMsg.setData("answer".getBytes());
        replyMsg.setSubject("_INBOX.DEADBEEF");
        when(mockSub.nextMessage(any(long.class), any(TimeUnit.class))).thenReturn(replyMsg);
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            when(c.newInbox()).thenReturn("_INBOX.DEADBEEF");
            when(c.subscribeSync(any(String.class), eq((String) null))).thenReturn(mockSub);
            Message msg = c.request("foo", null);
            assertEquals(replyMsg, msg);

            msg = c.request("foo", null, 1, TimeUnit.SECONDS);
            assertEquals(replyMsg, msg);

            msg = c.request("foo", null, 500);
            assertEquals(replyMsg, msg);

            // test for invalid
            msg = c.request("foo", null, -1);
            assertEquals(replyMsg, msg);


        }
    }

    @Test
    public void testSendSubscriptionMessage() throws IOException, TimeoutException {
        try (ConnectionImpl c = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            SubscriptionImpl mockSub = mock(SubscriptionImpl.class);
            when(mockSub.getQueue()).thenReturn("foo");
            when(mockSub.getSid()).thenReturn(55L);

            // Make sure sub isn't sent if reconnecting
            c.status = ConnState.RECONNECTING;
            c.setOutputStream(bwMock);
            c.sendSubscriptionMessage(mockSub);
            verify(bwMock, times(0)).write(any(byte[].class));

            // Ensure bw write error is logged
            c.status = ConnState.CONNECTED;
            doThrow(new IOException("test")).when(bwMock).write(any(byte[].class));
            c.sendSubscriptionMessage(mockSub);
            verify(bwMock, times(1)).write(any(byte[].class));
            verifier.verifyLogMsgEquals(Level.WARN,
                    "nats: I/O exception while sending subscription message");
        }
    }

    @Test
    public void testGetSetInputStream() throws IOException, TimeoutException {
        try (ConnectionImpl conn = (ConnectionImpl) Mockito.spy(newMockedConnection())) {
            conn.setInputStream(brMock);
            assertEquals(brMock, conn.getInputStream());
        }
    }

    @Test
    public void testGetSetHandlers() throws IOException, TimeoutException {
        ClosedCallback ccb = mock(ClosedCallback.class);
        DisconnectedCallback dcb = mock(DisconnectedCallback.class);
        ReconnectedCallback rcb = mock(ReconnectedCallback.class);
        ExceptionHandler eh = mock(ExceptionHandler.class);
        try (Connection c = newMockedConnection()) {
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


}
