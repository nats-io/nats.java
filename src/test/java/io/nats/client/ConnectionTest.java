package io.nats.client;


import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

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
			final AtomicBoolean disconnected = new AtomicBoolean(false);
			final Object mu = new Object();
	
			ConnectionFactory cf = new ConnectionFactory();
			cf.setReconnectAllowed(false);
			cf.setDisconnectedEventHandler(new DisconnectedEventHandler()
			{
				@Override
				public void onDisconnect(ConnectionEvent event) {
					synchronized (mu)
					{
						disconnected.set(true);
						mu.notify();
					} 
				}
			});
	
			Connection c = cf.createConnection();
			synchronized(mu)
			{
				UnitTestUtilities.bounceDefaultServer(1000);
				try {
					System.out.println("waiting for 10 sec for mu");
					long t0=System.nanoTime();
					mu.wait(10000);
					System.out.printf("waited %d sec for mu\n", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime()-t0));
				} catch (InterruptedException e) {}
			}
			c.close();
			assertTrue("disconnected should be 'true'.", disconnected.get());
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
}
