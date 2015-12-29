package io.nats.client;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.nats.client.Constants.*;

public class ConnectionTest {
	final Logger logger = LoggerFactory.getLogger(ConnectionTest.class);

	@Rule
	public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

	ExecutorService executor = Executors.newFixedThreadPool(5);
	//	UnitTestUtilities utils = new UnitTestUtilities();

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
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}


	@Test
	public void testConnectionStatus() throws IOException, TimeoutException
	{
		Connection c = new ConnectionFactory().createConnection();
		assertEquals(ConnState.CONNECTED, c.getState());
		c.close();
		assertEquals(ConnState.CLOSED, c.getState());
	}

	@Test
	public void testCloseHandler() throws IOException, TimeoutException
	{
		final AtomicBoolean closed = new AtomicBoolean(false);

		ConnectionFactory cf = new ConnectionFactory();
		cf.setClosedEventHandler(new ClosedEventHandler() {

			@Override
			public void onClose(ConnectionEvent event) {
				closed.set(true);
			}

		});
		Connection c = cf.createConnection();
		c.close();
		assertTrue("closed should equal 'true'.", closed.get());
	}

	@Test
	public void testCloseDisconnectedHandler() 
			throws IOException, TimeoutException
	{
		final AtomicBoolean disconnected = new AtomicBoolean(false);
		final Object disconnectedLock = new Object();

		ConnectionFactory cf = new ConnectionFactory();
		cf.setReconnectAllowed(false);
		cf.setDisconnectedEventHandler(new DisconnectedEventHandler()
		{
			@Override
			public void onDisconnect(ConnectionEvent event) {
				logger.trace("in disconnectedCB");
				synchronized (disconnectedLock)
				{
					System.err.println("disconnectedCB notifying");
					disconnected.set(true);
					disconnectedLock.notify();
				} 
			}
		});

		Connection c = cf.createConnection();
		assertFalse(c.isClosed());
		assertTrue(c.getState()==ConnState.CONNECTED);
		logger.info("Closing connection");
		c.close();
		assertTrue(c.isClosed());
		logger.info("Closed connection");
		synchronized (disconnectedLock)
		{
			try {
				logger.info("Waiting for disconnectedCB");
				disconnectedLock.wait(500);
				assertTrue("disconnectedCB not triggered.", disconnected.get());
			} catch (InterruptedException e) { System.err.println("Interrupted");}
		}

	}

	@Test
	public void testServerStopDisconnectedHandler() 
			throws IOException, TimeoutException
	{
		final Lock disconnectLock = new ReentrantLock();
		final Condition hasBeenDisconnected = disconnectLock.newCondition();

		ConnectionFactory cf = new ConnectionFactory();
		cf.setReconnectAllowed(false);
		cf.setDisconnectedEventHandler(new DisconnectedEventHandler()
		{
			@Override
			public void onDisconnect(ConnectionEvent event) {
				System.err.println("onDisconnect fired");
				disconnectLock.lock();
				try
				{
					hasBeenDisconnected.signal();
					System.err.println("disconnected");
				} finally {
					disconnectLock.unlock();
				}
			}
		});

		try (Connection c = cf.createConnection())
		{
			assertFalse(c.isClosed());
			disconnectLock.lock();
			try {
				UnitTestUtilities.bounceDefaultServer(1000);
				assertTrue(hasBeenDisconnected.await(10, TimeUnit.SECONDS));
			} catch (InterruptedException e) {
			}
			finally {
				disconnectLock.unlock();
			}
		}	
	}

	@Test
	public void testClosedConnections() throws Exception
	{
		Connection c = new ConnectionFactory().createConnection();
		SyncSubscription s = c.subscribeSync("foo");

		c.close();

		// While we can annotate all the exceptions in the test framework,
		// just do it manually.

		boolean failed=true;

		try { c.publish("foo", null);
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}

		failed=true;
		try { c.publish(new Message("foo", null, null)); 
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}

		failed=true;
		try { c.subscribeAsync("foo");
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}

		failed=true;
		try { c.subscribeSync("foo");
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}

		failed=true;
		try { c.subscribeAsync("foo", "bar");
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}

		failed=true;
		try { c.subscribeSync("foo", "bar");
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}

		failed=true;
		try { c.request("foo", null);
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}

		failed=true;
		try { s.nextMessage();
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}

		failed=true;
		try { s.nextMessage(100);
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}

		failed=true;
		try { s.unsubscribe();
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}

		failed=true;
		try { s.autoUnsubscribe(1);
		} catch (Exception e) {
			assertTrue("Expected ConnectionClosedException", e instanceof ConnectionClosedException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}
	}

	/// TODO NOT IMPLEMENTED:
	/// TestServerSecureConnections
	/// TestErrOnConnectAndDeadlock
	/// TestErrOnMaxPayloadLimit
	@Test
	public void testErrOnMaxPayloadLimit() {
		long expectedMaxPayload = 10;
		String serverInfo = "INFO {\"server_id\":\"foobar\",\"version\":\"0.6.6\",\"go\":\"go1.5.1\",\"host\":\"%s\",\"port\":%d,\"auth_required\":false,\"ssl_required\":false,\"max_payload\":%d}\r\n";

		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			String infoString = (String.format(serverInfo, "mockserver", 2222, 
					expectedMaxPayload));
			System.err.println(infoString);
			mock.setServerInfoString(infoString);
			ConnectionFactory cf = new ConnectionFactory();
			try (Connection c = cf.createConnection(mock))
			{
				// Make sure we parsed max payload correctly
				assertEquals(c.getMaxPayload(), expectedMaxPayload);

				// Check for correct exception 
				boolean failed=false;
				try {
					c.publish("hello", "hello world".getBytes());
				} catch (MaxPayloadException e) {
					failed = true;
				}
				finally {
					assertTrue("Should have generated a MaxPayloadException.", failed);
				}

				// Check for success on less than maxPayload

			} catch (IOException | TimeoutException e) {
				//				e.printStackTrace();
				fail("Connection to mock server failed: " + e.getMessage());
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}
	
	@Test
	public void testGetPropertiesFailure() {
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				Properties props = c.getProperties("foobar.properties");
				assertNull(props);
				
				InputStream is = mock(InputStream.class);
				doThrow(new IOException("Foo")).when(is).read(any(byte[].class));
				doThrow(new IOException("Foo")).when(is).read(any(byte[].class), any(Integer.class), any(Integer.class));;
				doThrow(new IOException("Foo")).when(is).read();

				props = c.getProperties(is);
				assertNull("getProperties() should have returned null", props);
				System.err.println("Success.");				
			} catch (IOException e) {
				fail("Should not have thrown any exception");
			} 
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testGetPropertiesSuccess() {
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				Properties props = c.getProperties("jnats.properties");
				assertNotNull(props);
				String version = props.getProperty("client.version");
				assertNotNull(version);
				System.out.println("version: " + version);
			} catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			} 
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCreateConnFailure() {
		boolean exThrown = false;
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			mock.setOpenFailure(true);
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				assertTrue(c.isClosed());
			} catch (IOException | TimeoutException e) {
				exThrown = true;
				assertTrue(e instanceof NoServersException);
			} 
			assertTrue("Should have thrown exception.", exThrown);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test 
	public void testGetServerInfo() {
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				assertTrue(!c.isClosed());
				ServerInfo info = c.getConnectedServerInfo();
				assertEquals("0.0.0.0",info.getHost());
				assertEquals("0.7.2", info.getVersion());
				assertEquals(4222, info.getPort());
				assertFalse(info.isAuthRequired());
				assertFalse(info.isTlsRequired());
				assertEquals(1048576, info.getMaxPayload());
			} catch (IOException | TimeoutException e) {
				fail("Should not have thrown exception");
				e.printStackTrace();
			} 
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testFlushFailure() {
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
//			mock.setBadWriter(true);
			boolean exThrown = false;
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				assertFalse(c.isClosed());
				c.close();
				try {
					c.flush(-1);
				} catch (IllegalArgumentException e) {
					exThrown=true;
				}
				assertTrue(exThrown);

				exThrown=false;
				try {
					c.flush();
				} catch (ConnectionClosedException e) {
					exThrown=true;
				}
				assertTrue(exThrown);

				exThrown = false;
				try {
					mock.setNoPongs(true);
					c.flush(5000);
				} catch (ConnectionClosedException e) {
					System.err.println("timeout connection closed");
					exThrown=true;
				} catch (Exception e) {
					fail("Wrong exception");
					e.printStackTrace();
				}
				assertTrue(exThrown);
				
			} catch (IOException | TimeoutException e) {
				fail("Exception thrown");
			} 
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testFlushFailureNoPong() {
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
//			mock.setBadWriter(true);
			boolean exThrown = false;
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				assertFalse(c.isClosed());
				exThrown = false;
				try {
					mock.setNoPongs(true);
					c.flush(2000);
				} catch (TimeoutException e) {
					System.err.println("timeout connection closed");
					exThrown=true;
				} catch (Exception e) {
					fail("Wrong exception: " + e.getClass().getName());
					Throwable cause = e.getCause();
					if (cause != null)
						cause.printStackTrace();
					e.printStackTrace();
				}
				assertTrue(exThrown);
				
			} catch (IOException | TimeoutException e) {
				fail("Exception thrown");
			} 
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	@Test
	public void testBadSid() {
		try (TCPConnectionMock mock = new TCPConnectionMock())
		{
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				assertFalse(c.isClosed());
				try {
					mock.deliverMessage("foo", 27, null, "Hello".getBytes());
				} catch (Exception e) {
					fail("Shouldn't have thrown an exception.");
					e.printStackTrace();
				}
				
			} catch (IOException | TimeoutException e) {
				fail("Exception thrown");
			} 
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testProcessMsgArgsErrors() {
		String tooFewArgsString = "foo bar";
		byte[] args = tooFewArgsString.getBytes();
		
		boolean exThrown = false;
		try (TCPConnectionMock mock = new TCPConnectionMock()) {
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				c.processMsgArgs(args, args.length);
			} catch (ParserException e) {
				exThrown = true;
				assertTrue(e.getMessage().startsWith("Unable to parse message arguments: "));
			} finally {
				assertTrue("Should have thrown ParserException", exThrown);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		String badSizeString = "foo 1 -1";
		args = badSizeString.getBytes();
		
		exThrown = false;
		try (TCPConnectionMock mock = new TCPConnectionMock()) {
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				c.processMsgArgs(args, args.length);
			} catch (ParserException e) {
				exThrown = true;
				assertTrue(e.getMessage().startsWith("Invalid Message - Bad or Missing Size: "));
			} finally {
				assertTrue("Should have thrown ParserException", exThrown);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
