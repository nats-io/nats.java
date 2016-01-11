/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
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
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Constants.ConnState;

@Category(UnitTest.class)
public class ClusterTest {
	final static Logger logger = LoggerFactory.getLogger(ClusterTest.class);
	
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
			try (Connection c = cf.createConnection()) {
				assertTrue(String.format("%s != %s", testServers[0], c.getConnectedUrl()),
						testServers[0].equals(c.getConnectedUrl()));
			} catch (IOException | TimeoutException e) {
				throw e;
			}
		} catch (Exception e1) {
			fail(e1.getMessage());
		}

		// make sure we can connect to a non-first server.
		try (NATSServer ns = utils.createServerOnPort(1227)) {
			try (Connection c = cf.createConnection()) {
				assertTrue(testServers[5] + " != " + c.getConnectedUrl(), testServers[5].equals(c.getConnectedUrl()));
			} catch (IOException | TimeoutException e) {
				throw e;
			}
		} catch (Exception e1) {
			fail(e1.getMessage());
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

		try (NATSServer as1 = utils.createServerWithConfig("auth_1222.conf")) {
			try (NATSServer as2 = utils.createServerWithConfig("auth_1224.conf")) {
				boolean exThrown=false;
				try (Connection c = cf.createConnection()) {
					assertNotNull(c.getConnectedUrl());
				} catch (Exception e) {
					assertTrue("Expected IOException", e instanceof IOException);
					assertNotNull(e.getMessage());
					assertTrue("Wrong error, wanted Auth failure, got '" + e.getMessage() +  "'",
							e.getMessage().contains("Authorization"));
					exThrown=true;
				} finally {
					assertTrue("Expect Auth failure, got no error\n", exThrown);
				}


				// Test that we can connect to a subsequent correct server.
				String[] authServers = new String[] {
						"nats://localhost:1222",
				"nats://username:password@localhost:1224"};

				cf.setServers(authServers);

				try (Connection c = cf.createConnection()) {
					assertTrue(c.getConnectedUrl().equals(authServers[1]));
				} catch (IOException | TimeoutException e) {
					if (e.getMessage().contains("Authorization")) {
						// ignore Authorization Failure
					}
					else {
						fail("Expected to connect properly: " + e.getMessage());
					}
				} catch (Exception e) {
					e.printStackTrace();
					fail(e.getMessage());
				}
			} // as2
		} // as1
	}

	@Test
	public void testBasicClusterReconnect() throws IOException, TimeoutException
	{
		try (NATSServer s1 = utils.createServerOnPort(1222)) {
			try (NATSServer s2 = utils.createServerOnPort(1224)) {

				//		final AtomicBoolean disconnected = new AtomicBoolean(false);
				//		final AtomicBoolean reconnected = new AtomicBoolean(false);
				String[] plainServers = new String[] {
						"nats://localhost:1222",
						"nats://localhost:1224"
				};

				final AtomicLong disconTimestamp = new AtomicLong(0L);
				final AtomicLong reconTimestamp = new AtomicLong(0L);
				final ConnectionFactory cf = new ConnectionFactory(plainServers);
				cf.setMaxReconnect(2);
				cf.setReconnectWait(1000);
				cf.setNoRandomize(true);

				final Lock disconnectLock = new ReentrantLock();
				final Condition disconnected = disconnectLock.newCondition();
				cf.setDisconnectedCallback(new DisconnectedCallback() {
					@Override
					public void onDisconnect(ConnectionEvent event) {
						disconnectLock.lock(); 
						try {
							disconTimestamp.set(System.nanoTime());
							// Suppress any additional calls
							event.getConnection().setDisconnectedCallback(null);
							disconnected.signal();
						}
						finally {
							disconnectLock.unlock();
						}
					}

				});

				final Lock reconnectLock = new ReentrantLock();
				final Condition reconnected = reconnectLock.newCondition();
				cf.setReconnectedCallback(new ReconnectedCallback() {
					@Override
					public void onReconnect(ConnectionEvent event) {
						reconTimestamp.set(System.nanoTime());
						reconnectLock.lock();
						try
						{
							event.getConnection().setReconnectedCallback(null);
							reconnected.signal();
						} finally {
							reconnectLock.unlock();
						}
					}
				});

				cf.setConnectionTimeout(200);

				try (Connection c = cf.createConnection()) {
					assertNotNull(c.getConnectedUrl());

					s1.shutdown();

					disconnectLock.lock();
					try
					{
						try {
							assertTrue("Timed out awaiting disconnect", disconnected.await(2, TimeUnit.SECONDS));
						} catch (InterruptedException e) {
						}
					} finally {
						disconnectLock.unlock();
					}

					reconnectLock.lock();
					try
					{
						try {
							assertTrue("Timed out awaiting reconnect", reconnected.await(2, TimeUnit.SECONDS));
						} catch (InterruptedException e) {
						}

					} finally {
						reconnectLock.unlock();
					}

					assertTrue(c.getConnectedUrl().equals(testServers[2]));

					c.close();
					s2.shutdown();

					long reconElapsed = TimeUnit.NANOSECONDS.toMillis(
							reconTimestamp.get() - disconTimestamp.get());
					assertTrue(reconElapsed <= cf.getReconnectWait());
					//		System.err.println("elapsed = " + reconElapsed);
					//		System.err.println("reconnectWait = " + cf.getReconnectWait());
				}
			}
		}
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
			cf.setReconnectedCallback(new ReconnectedCallback()
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
			} catch (IOException | TimeoutException e) {
				fail(e.getMessage());
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


	//TODO complete this
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
	public void testProperReconnectDelay() throws Exception
	{
		//		final Object mu = new Object();
		final Lock lock = new ReentrantLock();
		ConnectionFactory cf = new ConnectionFactory();
		cf.setServers(testServers);
		cf.setNoRandomize(true);

		//		final AtomicBoolean disconnectHandlerCalled = new AtomicBoolean(false);
		final Condition disconnectHandlerCalled = lock.newCondition();
		cf.setDisconnectedCallback( new DisconnectedCallback() {

			@Override
			public void onDisconnect(ConnectionEvent event) {
				event.getConnection().setDisconnectedCallback(null);
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

		cf.setClosedCallback(new ClosedCallback()
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
						assertTrue("DisconnectedCallback not called within timeout.",
								disconnectHandlerCalled.await(2, TimeUnit.SECONDS));
					} catch (InterruptedException e1) {
					}

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
	public void testProperFalloutAfterMaxAttempts() throws Exception
	{
		ConnectionFactory cf = new ConnectionFactory();

		cf.setServers(testServersShortList);
		cf.setNoRandomize(true);
		cf.setMaxReconnect(5);
		cf.setReconnectWait(25); // millis
		cf.setConnectionTimeout(500);

		final AtomicBoolean dcbCalled = new AtomicBoolean(false);

		final Channel<Boolean> dch = new Channel<Boolean>();
		cf.setDisconnectedCallback(new DisconnectedCallback()
		{
			public void onDisconnect(ConnectionEvent event) {
				dcbCalled.set(true);
				dch.add(true);
			}
		});

		final AtomicBoolean closedCbCalled = new AtomicBoolean(false);
		final Channel<Boolean> cch = new Channel<Boolean>();
		
		cf.setClosedCallback(new ClosedCallback()
		{
			public void onClose(ConnectionEvent event) {
				closedCbCalled.set(true);
				cch.add(true);
			}
		});

		try (NATSServer s1 = utils.createServerOnPort(1222))
		{
			try (Connection c = cf.createConnection())
			{
				s1.shutdown();

				// wait for disconnect
				assertTrue("Did not receive a disconnect callback message",
						dch.get(2,TimeUnit.SECONDS));

				// Wait for ClosedCB
				assertTrue("Did not receive a closed callback message",
						cch.get(2,TimeUnit.SECONDS));

				// Make sure we are not still reconnecting.
				assertTrue("Closed CB was not triggered, should have been.",
						closedCbCalled.get());

				// Expect connection to be closed...
				assertTrue("Wrong status: " + c.getState(), c.isClosed());
			}
		}
	}

	@Test
	public void testTimeoutOnNoServers()
	{
		final ConnectionFactory cf = new ConnectionFactory();
		cf.setServers(testServers);
		cf.setNoRandomize(true);
		cf.setMaxReconnect(2);
		cf.setReconnectWait(100); // millis

		final AtomicBoolean dcbCalled = new AtomicBoolean(false);

		final Channel<Boolean> dch = new Channel<Boolean>();
		cf.setDisconnectedCallback(new DisconnectedCallback() {
			public void onDisconnect(ConnectionEvent ev) {
				Connection conn = ev.getConnection();
				conn.setDisconnectedCallback(null);
				dcbCalled.set(true);
				dch.add(true);
			}
		});

		final Channel<Boolean> cch = new Channel<Boolean>();
		cf.setClosedCallback(new ClosedCallback() {
			public void onClose(ConnectionEvent ev) {
				cch.add(true);
			}
		});

		try (NATSServer s1 = new NATSServer(1222)) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e1) {
			}

			try (Connection c = cf.createConnection())
			{
				assertNotNull(c.getDisconnectedCallback());
				s1.shutdown();
				while(dch.getCount()!=1) {
					UnitTestUtilities.sleep(50);
				}
				// wait for disconnect
				assertTrue("Did not receive a disconnect callback message", 
						dch.get(5000)); 

				long t0 = System.nanoTime();

				// Wait for ClosedCB
				assertTrue("Did not receive a closed callback message", 
						cch.get(5000));

				long elapsedMsec = TimeUnit.NANOSECONDS.toMillis(
						System.nanoTime() - t0);

				// Use 500ms as variable time delta
				int variable = 500000;
				long expected = cf.getMaxReconnect() * cf.getReconnectWait();

				assertFalse("Waited too long for Closed state: " 
						+ elapsedMsec,
						elapsedMsec > (expected + variable));
			} catch (IOException | TimeoutException e) {
				e.printStackTrace();
				fail(e.getMessage());			
			}
		}
	}

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
	//			cf.setDisconnectedCallback(new DisconnectedCallback()
	//			{
	//				@Override
	//				public void onDisconnect(ConnectionEvent event) {
	//					disconnectTime.set(System.nanoTime());
	//				}
	//			});
	//	
	//			cf.setReconnectedCallback(new ReconnectedCallback()
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
