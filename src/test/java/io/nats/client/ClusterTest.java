package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

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
		Connection c = null;
		ConnectionFactory cf = new ConnectionFactory();
		cf.setNoRandomize(true);

		cf.setServers(testServers);

		// Make sure we can connect to first server if running
		NATSServer ns = utils.createServerOnPort(1222);
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			c = cf.createConnection();
			assertTrue(String.format("%s != %s", testServers[0], c.getConnectedUrl()),
					testServers[0].equals(c.getConnectedUrl()));
		} catch (IOException e) {
			throw e;
		} catch (TimeoutException e) {
			throw e;
		} 
		finally {
			c.close();
			ns.shutdown();
		}

		// make sure we can connect to a non-first server.
		ns = utils.createServerOnPort(1227);
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			c = cf.createConnection();
			assertTrue(testServers[5] + " != " + c.getConnectedUrl(), testServers[5].equals(c.getConnectedUrl()));
		} catch (IOException e) {
			throw e;
		} catch (TimeoutException e) {
			throw e;
		} 
		finally {
			c.close();
			ns.shutdown();
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

		Connection c = null;
		boolean failed=true;
		try { 
			c = cf.createConnection();
			if (c != null)
				System.err.println("Connected URL = " + c.getConnectedUrl());
		} catch (Exception e) {
			assertTrue("Expected IOException", e instanceof IOException);
			failed=false;
		} finally {
			if (c != null)
				c.close();
			assertFalse("Didn't throw an exception", failed);
		}


		// Test that we can connect to a subsequent correct server.
		String[] authServers = new String[] {
				"nats://localhost:1222",
		"nats://username:password@localhost:1224"};
		System.out.println("\n");
		cf.setServers(authServers);

		try {Thread.sleep(500);} catch (InterruptedException e) {}
		try {
			c = cf.createConnection();
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
			assertTrue(c.getConnectedUrl().equals(authServers[1]));
			c.close();
			as1.shutdown();
			as2.shutdown();
		}
	}

	@Test
	public void testBasicClusterReconnect() throws IOException, TimeoutException
	{
		final AtomicBoolean disconnected = new AtomicBoolean(false);
		final AtomicBoolean reconnected = new AtomicBoolean(false);
		String[] plainServers = new String[] {
				"nats://localhost:1222",
				"nats://localhost:1224"
		};

		final ConnectionFactory cf = new ConnectionFactory(plainServers);
		cf.setMaxReconnect(2);
		cf.setReconnectWait(1000);
		cf.setNoRandomize(true);

		final Object disconnectLock = new Object();
		cf.setDisconnectedEventHandler(new DisconnectedEventHandler() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				// Suppress any additional calls
				System.out.println("In onDisconnect");
				synchronized(disconnectLock) {
					event.getConnection().setDisconnectedEventHandler(null);
					disconnected.set(true);
					disconnectLock.notify();
				}
			}

		});

		final Object reconnectLock = new Object();
		cf.setReconnectedEventHandler(new ReconnectedEventHandler() {
			@Override
			public void onReconnect(ConnectionEvent event) {
				System.out.println("In onReconnect");
				synchronized(reconnectLock) {
					event.getConnection().setReconnectedEventHandler(null);
					reconnected.set(true);
					reconnectLock.notify();
				}
			}

		});

		cf.setConnectionTimeout(200);

		NATSServer 	s1 = utils.createServerOnPort(1222),
				s2 = utils.createServerOnPort(1224);
		try {Thread.sleep(1000);} catch (InterruptedException e){}
		Connection c = cf.createConnection();

		System.out.println("Connected to: " + c.getConnectedUrl());
		synchronized(disconnectLock)
		{
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e1) {}
			s1.shutdown();
			long t0=System.nanoTime();
			try {
				disconnectLock.wait(20000);
			} catch (InterruptedException e) {}
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
			assertTrue("disconnect lock waited "+elapsed+"msec", elapsed < 20000);
		}

		synchronized(reconnectLock)
		{
			long thist0 = System.nanoTime();
			try {
				reconnectLock.wait(20000);
			} catch (InterruptedException e) {}
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - thist0);
			assertTrue("reconnect lock waited " +elapsed+"msec", elapsed < 20000);

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
		//   Assert.Fail("Reconnect time took to long: {0} millis.",
		//        reconnectSw.ElapsedMilliseconds);
		//}
	}

	private class SimClient
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
	}


	//	//[TestMethod]
	//	public void TestHotSpotReconnect()
	//	{
	//		int numClients = 10;
	//		SimClient[] clients = new SimClient[100];
	//
	//		Options opts = ConnectionFactory.GetDefaultOptions();
	//		opts.Servers = testServers;
	//
	//		NATSServer s1 = utils.CreateServerOnPort(1222);
	//		Task[] waitgroup = new Task[numClients];
	//
	//
	//		for (int i = 0; i < numClients; i++)
	//		{
	//			clients[i] = new SimClient();
	//			Task t = new Task(() => {
	//				clients[i].Connect(testServers); 
	//				clients[i].waitForReconnect();
	//			});
	//			t.Start();
	//			waitgroup[i] = t;
	//		}
	//
	//
	//		NATSServer s2 = utils.CreateServerOnPort(1224);
	//		NATSServer s3 = utils.CreateServerOnPort(1226);
	//
	//		s1.Shutdown();
	//		Task.WaitAll(waitgroup);
	//
	//		int s2Count = 0;
	//		int s3Count = 0;
	//		int unknown = 0;
	//
	//		for (int i = 0; i < numClients; i++)
	//		{
	//			if (testServers[3].Equals(clients[i].ConnectedUrl))
	//				s2Count++;
	//			else if (testServers[5].Equals(clients[i].ConnectedUrl))
	//				s3Count++;
	//			else
	//				unknown++;
	//		}
	//
	//		Assert.IsTrue(unknown == 0);
	//		int delta = Math.Abs(s2Count - s3Count);
	//		int range = numClients / 30;
	//		if (delta > range)
	//		{
	//			Assert.Fail("Connected clients to servers out of range: {0}/{1}", delta, range);
	//		}
	//
	//	}
	//
	//	[TestMethod]
	//			public void TestProperReconnectDelay()
	//	{
	//		Object mu = new Object();
	//		Options opts = ConnectionFactory.GetDefaultOptions();
	//		opts.Servers = testServers;
	//		opts.NoRandomize = true;
	//
	//		bool disconnectHandlerCalled = false;
	//		opts.DisconnectedEventHandler = (sender, args) =>
	//		{
	//			opts.DisconnectedEventHandler = null;
	//			disconnectHandlerCalled = true;
	//			lock (mu)
	//			{
	//				disconnectHandlerCalled = true;
	//				Monitor.Pulse(mu);
	//			}
	//		};
	//
	//		bool closedCbCalled = false;
	//		opts.ClosedEventHandler = (sender, args) =>
	//		{
	//			closedCbCalled = true;
	//		};
	//
	//		using (NATSServer s1 = utils.CreateServerOnPort(1222))
	//		{
	//			IConnection c = new ConnectionFactory().CreateConnection(opts);
	//
	//			lock (mu)
	//			{
	//				s1.Shutdown();
	//				// wait for disconnect
	//				Assert.IsTrue(Monitor.Wait(mu, 10000));
	//
	//
	//				// Wait, want to make sure we don't spin on
	//				//reconnect to non-existant servers.
	//				Thread.Sleep(1000);
	//
	//				Assert.IsFalse(closedCbCalled);
	//				Assert.IsTrue(disconnectHandlerCalled);
	//				Assert.IsTrue(c.State == ConnState.RECONNECTING);
	//			}
	//
	//		}
	//	}
	//
	//	[TestMethod]
	//			public void TestProperFalloutAfterMaxAttempts()
	//	{
	//		Options opts = ConnectionFactory.GetDefaultOptions();
	//
	//		Object dmu = new Object();
	//		Object cmu = new Object();
	//
	//		opts.Servers = this.testServersShortList;
	//		opts.NoRandomize = true;
	//		opts.MaxReconnect = 2;
	//		opts.ReconnectWait = 25; // millis
	//		opts.Timeout = 500;
	//
	//		bool disconnectHandlerCalled = false;
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
	//		bool closedHandlerCalled = false;
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
	//				lock (cmu)
	//				{
	//					if (!closedHandlerCalled)
	//						Assert.IsTrue(Monitor.Wait(cmu, 60000));
	//				}
	//
	//				Assert.IsTrue(disconnectHandlerCalled);
	//				Assert.IsTrue(closedHandlerCalled);
	//				Assert.IsTrue(c.IsClosed());
	//			}
	//		}
	//	}
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
	//	//[TestMethod]
	//	public void TestPingReconnect()
	//	{
	//		/// Work in progress
	//		int RECONNECTS = 4;
	//
	//		Options opts = ConnectionFactory.GetDefaultOptions();
	//		Object mu = new Object();
	//
	//		opts.Servers = testServersShortList;
	//		opts.NoRandomize = true;
	//		opts.ReconnectWait = 200;
	//		opts.PingInterval = 50;
	//		opts.MaxPingsOut = -1;
	//		opts.Timeout = 1000;
	//
	//
	//		Stopwatch disconnectedTimer = new Stopwatch();
	//
	//		opts.DisconnectedEventHandler = (sender, args) =>
	//		{
	//			disconnectedTimer.Reset();
	//			disconnectedTimer.Start();
	//		};
	//
	//		opts.ReconnectedEventHandler = (sender, args) =>
	//		{
	//			lock (mu)
	//			{
	//				args.Conn.Opts.MaxPingsOut = 500;
	//				disconnectedTimer.Stop();
	//				Monitor.Pulse(mu);
	//			}
	//		};
	//
	//		using (NATSServer s1 = utils.CreateServerOnPort(1222))
	//		{
	//			using (IConnection c = new ConnectionFactory().CreateConnection(opts))
	//			{
	//				s1.Shutdown();
	//				for (int i = 0; i < RECONNECTS; i++)
	//				{
	//					lock (mu)
	//					{
	//						Assert.IsTrue(Monitor.Wait(mu, 100000));
	//					}
	//				}
	//			}
	//		}
	//	}
}
