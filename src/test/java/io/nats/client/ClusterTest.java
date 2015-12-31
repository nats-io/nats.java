package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import io.nats.client.Constants.ConnState;

public class ClusterTest {
	@Rule
	public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		//		s = util.createServerWithConfig("auth_1222.conf");
	}

	@After
	public void tearDown() throws Exception {
		//		s.shutdown();
	}
	final static String[] testServers = new String[] {
			"nats://localhost:1222",
			"nats://localhost:1223",
			"nats://localhost:1224",
			"nats://localhost:1225",
			"nats://localhost:1226",
			"nats://localhost:1227",
			"nats://localhost:1228" 
	};

	final static String[] testServersShortList = new String[] {
			"nats://localhost:1222",
			"nats://localhost:1223"
	};

	UnitTestUtilities utils = new UnitTestUtilities();

	@Test
	public void testServersOption() throws IOException, TimeoutException
	{
		ConnectionFactory cf = new ConnectionFactory();
		cf.setNoRandomize(true);

		cf.setServers(testServers);

		// Make sure we can connect to first server if running
		try (NATSServer ns = utils.createServerOnPort(1222)) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			try (Connection c = cf.createConnection()) {
				assertTrue(String.format("%s != %s", testServers[0], c.getConnectedUrl()),
						testServers[0].equals(c.getConnectedUrl()));
			} catch (IOException | TimeoutException e) {
				throw e;
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// make sure we can connect to a non-first server.
		try (NATSServer ns = utils.createServerOnPort(1227)) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try (Connection c = cf.createConnection()) {
				assertTrue(testServers[5] + " != " + c.getConnectedUrl(), testServers[5].equals(c.getConnectedUrl()));
			} catch (IOException | TimeoutException e) {
				throw e;
			}
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Test
	public void testAuthServers() throws IOException, TimeoutException
	{
		String[] plainServers = new String[] {
				"nats://localhost:1222",
				"nats://localhost:1224"
		};

		ConnectionFactory cf = new ConnectionFactory();
		cf.setNoRandomize(true);
		cf.setServers(plainServers);
		cf.setConnectionTimeout(5000);

		NATSServer as1 = utils.createServerWithConfig("auth_1222.conf"),
				as2 = utils.createServerWithConfig("auth_1224.conf");

		try {Thread.sleep(500);} catch (InterruptedException e) {}

		boolean failed=true;
		try (Connection c = cf.createConnection()) {
			System.err.println("Connected URL = " + c.getConnectedUrl());
		} catch (Exception e) {
			assertTrue("Expected IOException", e instanceof IOException);
			failed=false;
		} finally {
			assertFalse("Didn't throw an exception", failed);
		}


		// Test that we can connect to a subsequent correct server.
		String[] authServers = new String[] {
				"nats://localhost:1222",
		"nats://username:password@localhost:1224"};
		System.out.println("\n");
		cf.setServers(authServers);

		try {Thread.sleep(500);} catch (InterruptedException e) {}
		try (Connection c = cf.createConnection()) {
			assertTrue(c.getConnectedUrl().equals(authServers[1]));
		} catch (IOException e) {
			if (e instanceof AuthorizationException) {
				System.out.println("ignoring AuthorizationException");
			}
			else
				throw e;
		} catch (TimeoutException e) {
			throw e;
		}
		finally {
			as1.shutdown();
			as2.shutdown();
		}
	}

	@Test
	public void testBasicClusterReconnect() throws IOException, TimeoutException
	{
		//		final AtomicBoolean disconnected = new AtomicBoolean(false);
		//		final AtomicBoolean reconnected = new AtomicBoolean(false);
		String[] plainServers = new String[] {
				"nats://localhost:1222",
				"nats://localhost:1224"
		};

		final ConnectionFactory cf = new ConnectionFactory(plainServers);
		cf.setMaxReconnect(2);
		cf.setReconnectWait(1000);
		cf.setNoRandomize(true);

		final Lock disconnectLock = new ReentrantLock();
		final Condition disconnected = disconnectLock.newCondition();
		cf.setDisconnectedEventHandler(new DisconnectedEventHandler() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				System.out.println("In onDisconnect");
				disconnectLock.lock(); 
				try {
					// Suppress any additional calls
					event.getConnection().setDisconnectedEventHandler(null);
					//					disconnected.set(true);
					disconnected.signal();
				}
				finally {
					disconnectLock.unlock();
				}
			}

		});

		final Lock reconnectLock = new ReentrantLock();
		final Condition reconnected = reconnectLock.newCondition();
		cf.setReconnectedEventHandler(new ReconnectedEventHandler() {
			@Override
			public void onReconnect(ConnectionEvent event) {
				System.out.println("In onReconnect");
				reconnectLock.lock();
				try
				{
					event.getConnection().setReconnectedEventHandler(null);
					reconnected.signal();
					//					reconnectLock.notify();
				} finally {
					reconnectLock.unlock();
				}
			}

		});

		cf.setConnectionTimeout(200);

		NATSServer 	s1 = utils.createServerOnPort(1222),
				s2 = utils.createServerOnPort(1224);
		try {Thread.sleep(1000);} catch (InterruptedException e){}
		Connection c = cf.createConnection();

		System.out.println("Connected to: " + c.getConnectedUrl());
		disconnectLock.lock();
		try
		{
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e1) {}
			s1.shutdown();

			try {
				assertTrue("Timed out awaiting disconnect", disconnected.await(20, TimeUnit.SECONDS));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//			long t0=System.nanoTime();
			//			try {
			//				disconnectLock.wait(20000);
			//			} catch (InterruptedException e) {}
			//			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
			//			assertTrue("disconnect lock waited "+elapsed+"msec", elapsed < 20000);
		} finally {
			disconnectLock.unlock();
		}

		reconnectLock.lock();
		try
		{
			try {
				assertTrue("Timed out awaiting reconnect", reconnected.await(20, TimeUnit.SECONDS));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			//			long thist0 = System.nanoTime();
			//			try {
			//				reconnectLock.wait(20000);
			//			} catch (InterruptedException e) {}
			//			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - thist0);
			//			assertTrue("reconnect lock waited " +elapsed+"msec", elapsed < 20000);

		} finally {
			reconnectLock.unlock();
		}

		assertTrue(c.getConnectedUrl().equals(testServers[2]));

		c.close();
		s2.shutdown();
		//reconnectSw.Stop();

		// Make sure we did not wait on reconnect for default time.
		// Reconnect should be fast since it will be a switch to the
		// second server and not be dependent on server restart time.
		// TODO:  .NET connect timeout is exceeding long compared to
		// GO's.  Look shortening it, or living with it.
		//if (reconnectSw.ElapsedMilliseconds > opts.ReconnectWait)
		//{
		//   Assert.Fail("Reconnect time took too long: {0} millis.",
		//        reconnectSw.ElapsedMilliseconds);
		//}
	}

	private class SimClient implements Runnable
	{
		Connection c;
		final Object mu = new Object();

		public String getConnectedUrl()
		{
			return c.getConnectedUrl();
		}

		public void waitForReconnect()
		{
			synchronized(mu)
			{
				try { mu.wait();
				} catch (InterruptedException e) {
				}
			}
		}

		public void connect(String[] servers)
		{
			ConnectionFactory cf = new ConnectionFactory();
			cf.setServers(servers);
			cf.setReconnectedEventHandler(new ReconnectedEventHandler()
			{
				@Override
				public void onReconnect(ConnectionEvent event) {
					synchronized(mu)
					{
						mu.notify();
					}
				}
			});
			try {
				c = cf.createConnection();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public void close()
		{
			c.close();
		}

		@Override
		public void run() {
			connect(testServers);
			waitForReconnect();
		}
	} //SimClient


	//	@Test
	//	public void TestHotSpotReconnect()
	//	{
	//		ExecutorService executor = 
	//				Executors.newCachedThreadPool(
	//						new NATSThreadFactory("testhotspotreconnect"));
	//		int numClients = 10;
	//		List<SimClient> clients = new ArrayList<SimClient>();
	//
	//		ConnectionFactory cf = new ConnectionFactory();
	//		cf.setServers(testServers);
	//
	//		NATSServer s1 = utils.createServerOnPort(1222);
	//		WaitGroup wg = new WaitGroup();
	//
	//
	//		for (int i = 0; i < numClients; i++)
	//		{
	//			SimClient client = new SimClient();
	//			executor.execute(client);
	//			clients.add(client);
	//			wg.add(1);
	//		}
	//
	//
	//		NATSServer s2 = utils.createServerOnPort(1224);
	//		NATSServer s3 = utils.createServerOnPort(1226);
	//
	//		s1.shutdown();
	//		try { wg.await(); } catch (InterruptedException e) {}
	//
	//		int s2Count = 0;
	//		int s3Count = 0;
	//		int unknown = 0;
	//
	//		for (int i = 0; i < numClients; i++)
	//		{
	//			if (testServers[2].equals(clients.get(i).getConnectedUrl()))
	//				s2Count++;
	//			else if (testServers[4].equals(clients.get(i).getConnectedUrl()))
	//				s3Count++;
	//			else
	//				unknown++;
	//		}
	//
	//		s2.shutdown();
	//		s3.shutdown();
	//
	//		assertTrue(unknown == 0);
	//		int delta = Math.abs(s2Count - s3Count);
	//		int range = numClients / 30;
	//		if (delta > range)
	//		{
	//			String s = String.format("Connected clients to servers out of range: %d/%d", 
	//					delta, range);
	//			fail(s);
	//		}
	//
	//
	//	}

	@Test
	public void TestProperReconnectDelay() throws Exception
	{
		//		final Object mu = new Object();
		final Lock lock = new ReentrantLock();
		ConnectionFactory cf = new ConnectionFactory();
		cf.setServers(testServers);
		cf.setNoRandomize(true);

		//		final AtomicBoolean disconnectHandlerCalled = new AtomicBoolean(false);
		final Condition disconnectHandlerCalled = lock.newCondition();
		cf.setDisconnectedEventHandler( new DisconnectedEventHandler() {

			@Override
			public void onDisconnect(ConnectionEvent event) {
				event.getConnection().setDisconnectedEventHandler(null);
				//				disconnectHandlerCalled.set(true);
				lock.lock();
				try
				{
					disconnectHandlerCalled.signal();
					//					mu.notify();
				} finally {
					lock.unlock();
				}
			}
		});

		//		final AtomicBoolean closedCbCalled = new AtomicBoolean(false);
		final AtomicBoolean closedCbCalled = new AtomicBoolean(false);

		cf.setClosedEventHandler(new ClosedEventHandler()
		{
			@Override
			public void onClose(ConnectionEvent event) {
				closedCbCalled.set(true);
			}
		});

		try (NATSServer s1 = utils.createServerOnPort(1222))
		{
			try (Connection c = cf.createConnection())
			{
				assertFalse(c.isClosed());

				lock.lock();
				try
				{
					s1.shutdown();
					// wait for disconnect
					try {
						System.err.println("Waiting for DisconnectedEventHandler");
						assertTrue("DisconnectedEventHandler not called withing timeout.",
								disconnectHandlerCalled.await(2, TimeUnit.SECONDS));
						System.err.println("DisconnectedEventHandler triggered successfully");
					} catch (InterruptedException e1) {
					}

					// Wait, want to make sure we don't spin on
					//reconnect to non-existant servers.
					try {Thread.sleep(1000);} catch (InterruptedException e) {}

					assertFalse("Closed CB was triggered, should not have been.",
							closedCbCalled.get());
					assertEquals(c.getState(), ConnState.RECONNECTING);
				} finally {
					lock.unlock();
				}

			}
		}
	}

	@Test
	public void TestProperFalloutAfterMaxAttempts() throws Exception
	{
		ConnectionFactory cf = new ConnectionFactory();
		final Lock dmu = new ReentrantLock();
		final Lock cmu = new ReentrantLock();

		cf.setServers(testServersShortList);
		cf.setNoRandomize(true);
		cf.setMaxReconnect(5);
		cf.setReconnectWait(25); // millis
		cf.setConnectionTimeout(500);

		final Condition disconnectHandlerCalled = dmu.newCondition();

		cf.setDisconnectedEventHandler(new DisconnectedEventHandler()
		{
			@Override
			public void onDisconnect(ConnectionEvent event) {
				dmu.lock();
				try
				{
					disconnectHandlerCalled.signal();
					//					Monitor.Pulse(dmu);
				} 
				finally {
					dmu.unlock();
				}
			}
		});

		final Condition closedHandlerCalled = cmu.newCondition();
		cf.setClosedEventHandler(new ClosedEventHandler()
		{
			@Override
			public void onClose(ConnectionEvent event) {
				cmu.lock();
				try
				{
					closedHandlerCalled.signal();
				} finally {
					cmu.unlock();
				}				
			}
		});

		try (NATSServer s1 = utils.createServerOnPort(1222))
		{
			try (Connection c = cf.createConnection())
			{
				s1.shutdown();

				dmu.lock();
				try
				{
					assertTrue("Did not receive a disconnect callback message",
							disconnectHandlerCalled.await(2,TimeUnit.SECONDS));
				} catch (InterruptedException e) {
				}
				finally {
					dmu.unlock();
				}

				cmu.lock();
				try
				{
					assertTrue("Did not receive a closed callback message",
							closedHandlerCalled.await(2,TimeUnit.SECONDS));
				} catch (InterruptedException e) {
				} finally {
					cmu.unlock();
				}

				//				Assert.IsTrue(disconnectHandlerCalled);
				//				Assert.IsTrue(closedHandlerCalled);
				assertTrue("Wrong status: " + c.getState(), c.isClosed());
			}
		}
	}
	//
	//	[TestMethod]
	//			public void TestTimeoutOnNoServers()
	//	{
	//		Options opts = ConnectionFactory.GetDefaultOptions();
	//		Object dmu = new Object();
	//		Object cmu = new Object();
	//
	//		opts.Servers = testServersShortList;
	//		opts.NoRandomize = true;
	//		opts.MaxReconnect = 2;
	//		opts.ReconnectWait = 100; // millis
	//
	//		bool disconnectHandlerCalled = false;
	//		bool closedHandlerCalled = false;
	//
	//		opts.DisconnectedEventHandler = (sender, args) =>
	//		{
	//			lock (dmu)
	//			{
	//				disconnectHandlerCalled = true;
	//				Monitor.Pulse(dmu);
	//			}
	//		};
	//
	//		opts.ClosedEventHandler = (sender, args) =>
	//		{
	//			lock (cmu)
	//			{
	//				closedHandlerCalled = true;
	//				Monitor.Pulse(cmu);
	//			}
	//		};
	//
	//		using (NATSServer s1 = utils.CreateServerOnPort(1222))
	//		{
	//			using (IConnection c = new ConnectionFactory().CreateConnection(opts))
	//			{
	//				s1.Shutdown();
	//
	//				lock (dmu)
	//				{
	//					if (!disconnectHandlerCalled)
	//						Assert.IsTrue(Monitor.Wait(dmu, 20000));
	//				}
	//
	//				Stopwatch sw = new Stopwatch();
	//				sw.Start();
	//
	//				lock (cmu)
	//				{
	//					if (!closedHandlerCalled)
	//						Assert.IsTrue(Monitor.Wait(cmu, 60000));
	//				}
	//
	//				sw.Stop();
	//
	//				int expected = opts.MaxReconnect * opts.ReconnectWait;
	//
	//				// .NET has long connect times, so revisit this after
	//				// a connect timeout has been added.
	//				//Assert.IsTrue(sw.ElapsedMilliseconds < (expected + 500));
	//
	//				Assert.IsTrue(disconnectHandlerCalled);
	//				Assert.IsTrue(closedHandlerCalled);
	//				Assert.IsTrue(c.IsClosed());
	//			}
	//		}
	//	}
	//
	//		@Test
	//		public void TestPingReconnect() throws Exception
	//		{
	//			/// Work in progress
	//			int RECONNECTS = 4;
	//	
	//			ConnectionFactory cf = new ConnectionFactory();
	//			final Lock mu = new ReentrantLock();
	//			final Condition reconnected = mu.newCondition();
	//			
	//			cf.setServers(testServersShortList);
	//			cf.setNoRandomize(true);
	//			cf.setReconnectWait(200);
	//			cf.setPingInterval(50);
	//			cf.setMaxPingsOut(-1);
	////			cf.setConnectionTimeout(1000);
	//	
	//			final AtomicLong disconnectTime = new AtomicLong();
	//			final AtomicLong reconnectTime = new AtomicLong();
	//			final AtomicLong reconnectElapsed = new AtomicLong();
	//	
	////			Stopwatch disconnectedTimer = new Stopwatch();
	//	
	//			
	//			cf.setDisconnectedEventHandler(new DisconnectedEventHandler()
	//			{
	//				@Override
	//				public void onDisconnect(ConnectionEvent event) {
	//					disconnectTime.set(System.nanoTime());
	//				}
	//			});
	//	
	//			cf.setReconnectedEventHandler(new ReconnectedEventHandler()
	//			{
	//				@Override
	//				public void onReconnect(ConnectionEvent event) {
	//					mu.lock();
	//					try
	//					{
	//						reconnectTime.set(System.nanoTime());
	//						reconnectElapsed.set(TimeUnit.NANOSECONDS.toMillis(
	//								reconnectTime.get() - disconnectTime.get()));
	//						reconnected.signal();
	//					}
	//					finally {
	//						mu.unlock();
	//					}
	//				}
	//			});
	//	
	//			try (NATSServer s1 = utils.createServerOnPort(1222))
	//			{
	//				try (Connection c = cf.createConnection())
	//				{
	//					s1.shutdown();
	//					for (int i = 0; i < RECONNECTS; i++)
	//					{
	//						mu.lock();
	//						try
	//						{
	//							assertTrue(reconnected.await(2 * cf.getPingInterval(), TimeUnit.MILLISECONDS));
	//						}
	//						finally
	//						{
	//							mu.unlock();
	//						}
	//					}
	//				}
	//			}
	//		}

	public class WaitGroup {

		private int jobs = 0;

		public synchronized void add(int i) {
			jobs += i;
		}

		public synchronized void done() {
			if (--jobs == 0) {
				notifyAll();
			}
		}

		public synchronized void await() throws InterruptedException {
			while (jobs > 0) {
				wait();
			}
		}

	}

}
