/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.junit.Assert.*;
import static io.nats.client.UnitTestUtilities.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
					fail("Expect Auth failure, got no error");
				} catch (Exception e) {
					assertTrue("Expected IOException", e instanceof IOException);
					assertNotNull(e.getMessage());
					assertEquals(Constants.ERR_AUTHORIZATION, e.getMessage());
					exThrown=true;
				}
				assertTrue("Expect Auth failure, got no error", exThrown);


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
	public void testBasicClusterReconnect()
	{
		try (NATSServer s1 = utils.createServerOnPort(1222)) {
			try (NATSServer s2 = utils.createServerOnPort(1224)) {

				final AtomicLong disconTimestamp = new AtomicLong(0L);
				final AtomicLong reconElapsed = new AtomicLong(0L);
				final ConnectionFactory cf = new ConnectionFactory(testServers);
				cf.setMaxReconnect(2);
				cf.setReconnectWait(1000);
				cf.setNoRandomize(true);

				final Channel<Boolean> dch = new Channel<Boolean>();
				cf.setDisconnectedCallback(new DisconnectedCallback() {
					public void onDisconnect(ConnectionEvent event) {
						logger.trace("onDisconnect()");
						disconTimestamp.set(System.nanoTime());
						// Suppress any additional calls
						event.getConnection().setDisconnectedCallback(null);
						dch.add(true);
						logger.trace("signaled disconnect");
					}

				});

				final Channel<Boolean> rch = new Channel<Boolean>();
				cf.setReconnectedCallback(new ReconnectedCallback() {
					public void onReconnect(ConnectionEvent event) {
						logger.trace("onReconnect()");
						reconElapsed.set(TimeUnit.NANOSECONDS.toMillis(
								System.nanoTime() - disconTimestamp.get()));
						rch.add(true);
						logger.trace("signaled reconnect");
					}
				});

				try (Connection c = cf.createConnection()) {
					assertNotNull(c.getConnectedUrl());

					s1.shutdown();

					// wait for disconnect
					try {
						logger.trace("\n\nWaiting for disconnect signal\n\n");
						assertTrue("Did not receive a disconnect callback message", 
								dch.get(2, TimeUnit.SECONDS));
					} catch (TimeoutException e) {
						fail("Did not receive a disconnect callback message: " + e.getMessage());
					}
					sleep(3, TimeUnit.SECONDS);
					try {
						logger.trace("\n\nWaiting for reconnect signal\n\n");
						assertTrue("Did not receive a reconnect callback message: ", 
								rch.get(2, TimeUnit.SECONDS));
					} catch (TimeoutException e) {
						fail("Did not receive a reconnect callback message: " + e.getMessage());
					}

					assertTrue(c.getConnectedUrl().equals(testServers[2]));

					// Make sure we did not wait on reconnect for default time.
					// Reconnect should be fast since it will be a switch to the
					// second server and not be dependent on server restart time.
					//					assertTrue(reconElapsed.get() <= cf.getReconnectWait());

					assertTrue(reconElapsed.get() < 100);
				} catch (IOException | TimeoutException e) {
					fail(e.getMessage());
				}
			}
		}
	}


	@Test
	public void testHotSpotReconnect()
	{
		int numClients = 100;
		ExecutorService executor = 
				Executors.newFixedThreadPool(numClients,
						new NATSThreadFactory("testhotspotreconnect"));

		final Channel<String> rch = new Channel<String>();
		final Channel<Integer> dch = new Channel<Integer>();
		final AtomicBoolean shutdown = new AtomicBoolean(false);
		try (NATSServer s1 = utils.createServerOnPort(1222)) {
			try (NATSServer s2 = utils.createServerOnPort(1224)) {
				try (NATSServer s3 = utils.createServerOnPort(1226)) {
					
					final class NATSClient implements Runnable {
						ConnectionFactory cf = new ConnectionFactory();
						Connection nc = null;
						final AtomicInteger numReconnects = new AtomicInteger(0);
						final AtomicInteger numDisconnects = new AtomicInteger(0);
						String currentUrl = null;
						final AtomicInteger instance = new AtomicInteger(-1);
						NATSClient(int inst) {
							this.instance.set(inst);
							cf.setServers(testServers);
							cf.setDisconnectedCallback(new DisconnectedCallback() {
								public void onDisconnect(ConnectionEvent event) {
									numDisconnects.incrementAndGet();
									dch.add(instance.get());
									nc.setDisconnectedCallback(null);
								}
							});
							cf.setReconnectedCallback(new ReconnectedCallback() {
								public void onReconnect(ConnectionEvent event) {
									numReconnects.incrementAndGet();
									currentUrl = nc.getConnectedUrl();
									rch.add(currentUrl);
								}
							});
						}
						@Override public void run() {
							try
							{
								nc = cf.createConnection();
								assertTrue(!nc.isClosed());
								assertNotNull(nc.getConnectedUrl());
								currentUrl = nc.getConnectedUrl();
//								System.err.println("Instance " + instance + " connected to " + currentUrl);
								while(!shutdown.get()) {
									sleep(10);
								}
								nc.close();
							} catch (IOException | TimeoutException e) {
								e.printStackTrace();
							}
						}
						public synchronized boolean isConnected() {
							return (nc!=null && !nc.isClosed());
						}
						public void shutdown() {
							shutdown.set(true);
						}
					}

					List<NATSClient> tasks = new ArrayList<NATSClient>(numClients);
					for(int i=0; i< numClients; i++){
						NATSClient r = new NATSClient(i); 
						tasks.add(r);
						executor.submit(r);
					}
					
					Map<String, Integer> cs = new HashMap<String, Integer>();
					
					int numReady = 0;
					while (numReady < numClients) {
						numReady = 0;
						for (NATSClient cli : tasks) {
							if (cli.isConnected()) {
								numReady++;
							}
						}
						sleep(100);
					}

					s1.shutdown();
					sleep(1000);
					
					int disconnected = 0;
					// wait for disconnects
					while (dch.getCount()>0 && disconnected<numClients) {
						Integer instance = -1;
						try {
							instance = dch.get(5, TimeUnit.SECONDS);
							assertNotNull(instance);
						} catch (TimeoutException e) {
							fail("timed out waiting for disconnect signal");
						}
						disconnected++;
					}
					assertTrue(disconnected > 0);

	
					int reconnected = 0;
					// wait for reconnects
					for (int i=0; i<disconnected; i++) {
						String url = null;
						while (rch.getCount()==0) {
							sleep(50);
						}
						try {
							url = rch.get(5, TimeUnit.SECONDS);
						} catch (TimeoutException e) {
							fail("timed out waiting for reconnect signal");
						}
						assertNotNull(url);
						reconnected++;
						Integer count = cs.get(url);
						if (count != null)
							cs.put(url, ++count);
						else
							cs.put(url, new Integer(1));
					}

					for (NATSClient client : tasks) {
						client.shutdown();
					}
					executor.shutdown();
					try {
						assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					assertEquals(disconnected, reconnected);
					
					int numServers = 2;

					assertEquals(numServers, cs.size());
					
					int expected = numClients/numServers;
					// We expect a 40 percent variance
					int v = (int) ((float)expected * 0.40);

					int delta = Math.abs(cs.get(testServers[2]) - 
							cs.get(testServers[4]));
//					System.err.printf("v = %d, delta = %d\n", v, delta);
					if (delta > v)
					{
						String s = String.format("Connected clients to servers out of range: %d/%d", 
								delta, v);
						fail(s);
					}
				}
			}
		}
	}

	@Test
	public void testProperReconnectDelay() throws Exception
	{
		try (NATSServer s1 = utils.createServerOnPort(1222))
		{
			ConnectionFactory cf = new ConnectionFactory();
			cf.setServers(testServers);
			cf.setNoRandomize(true);

			final Channel<Boolean> dch = new Channel<Boolean>();
			cf.setDisconnectedCallback( new DisconnectedCallback() {
				public void onDisconnect(ConnectionEvent event) {
					event.getConnection().setDisconnectedCallback(null);
					dch.add(true);
				}
			});

			final AtomicBoolean ccbCalled = new AtomicBoolean(false);
			cf.setClosedCallback(new ClosedCallback()
			{
				public void onClose(ConnectionEvent event) {
					ccbCalled.set(true);
				}
			});

			try (Connection c = cf.createConnection())
			{
				assertFalse(c.isClosed());

				s1.shutdown();
				// wait for disconnect
				assertTrue("Did not receive a disconnect callback message",
						dch.get(2, TimeUnit.SECONDS));

				// Wait, want to make sure we don't spin on reconnect to non-existent servers.
				sleep (1, TimeUnit.SECONDS);

				assertFalse("Closed CB was triggered, should not have been.",
						ccbCalled.get());
				assertEquals("Wrong state: " + c.getState(),
						c.getState(), ConnState.RECONNECTING);
			}
		}
	}

	@Test
	public void testProperFalloutAfterMaxAttempts()
	{
		ConnectionFactory cf = new ConnectionFactory();

//		cf.setServers(testServersShortList);
		cf.setServers(testServers);
		cf.setNoRandomize(true);
		cf.setMaxReconnect(5);
		cf.setReconnectWait(25); // millis

		final Channel<Boolean> dch = new Channel<Boolean>();
		final AtomicBoolean dcbCalled = new AtomicBoolean(false);
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
				try {
					assertTrue("Did not receive a disconnect callback message",
							dch.get(2,TimeUnit.SECONDS));
				} catch (TimeoutException e) {
					fail("Did not receive a disconnect callback message");
				}

				// Wait for ClosedCB
				try {
					assertTrue("Did not receive a closed callback message",
							cch.get(2,TimeUnit.SECONDS));
				} catch (TimeoutException e) {
					fail("Did not receive a closed callback message");
				}

				// Make sure we are not still reconnecting.
				assertTrue("Closed CB was not triggered, should have been.",
						closedCbCalled.get());

				// Expect connection to be closed...
				assertTrue("Wrong status: " + c.getState(), c.isClosed());
			} catch (IOException | TimeoutException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	@Test
	public void testTimeoutOnNoServers()
	{
		final ConnectionFactory cf = new ConnectionFactory();
		cf.setServers(testServers);
		cf.setNoRandomize(true);
		
		// 1 second total time wait
		cf.setMaxReconnect(10);
		cf.setReconnectWait(100);

		final AtomicBoolean dcbCalled = new AtomicBoolean(false);

		final Channel<Boolean> dch = new Channel<Boolean>();
		cf.setDisconnectedCallback(new DisconnectedCallback() {
			public void onDisconnect(ConnectionEvent ev) {
				ev.getConnection().setDisconnectedCallback(null);
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
//				while(dch.getCount()!=1) {
//					UnitTestUtilities.sleep(50);
//				}
				// wait for disconnect
				assertTrue("Did not receive a disconnect callback message", 
						dch.get(2, TimeUnit.SECONDS)); 

				long t0 = System.nanoTime();

				// Wait for ClosedCB
				assertTrue("Did not receive a closed callback message", 
						cch.get(2, TimeUnit.SECONDS));

				long elapsedMsec = TimeUnit.NANOSECONDS.toMillis(
						System.nanoTime() - t0);

				// Use 500ms as variable time delta
				long variable = 500;
				long expected = cf.getMaxReconnect() * cf.getReconnectWait();

				assertFalse("Waited too long for Closed state: " + elapsedMsec,
						elapsedMsec > (expected + variable));
			} catch (IOException | TimeoutException e) {
				e.printStackTrace();
				fail(e.getMessage());			
			}
		}
	}

		// TODO this test, as written in Go, makes no sense
//		@Test
		public void testPingReconnect() throws Exception
		{
			int RECONNECTS = 4;
			try (NATSServer s1 = utils.createServerOnPort(1222))
			{
	
				ConnectionFactory cf = new ConnectionFactory();
	
				cf.setServers(testServers);
				cf.setNoRandomize(true);
				cf.setReconnectWait(200);
				cf.setPingInterval(50);
				cf.setMaxPingsOut(-1);
				cf.setConnectionTimeout(1000);
	
				final Channel<Long> rch = new Channel<Long>(RECONNECTS);
				final Channel<Long> dch = new Channel<Long>(RECONNECTS);
	
				cf.setDisconnectedCallback(new DisconnectedCallback()
				{
					public void onDisconnect(ConnectionEvent event) {
						dch.add(System.nanoTime());
						System.err.println("onDisconnect(), disconnected from " + event.nc.getConnectedUrl());
					}
				});
	
				cf.setReconnectedCallback(new ReconnectedCallback()
				{
					@Override
					public void onReconnect(ConnectionEvent event) {
						rch.add(System.nanoTime());
						System.err.println("onReconnect()");
					}
				});
	
				try (ConnectionImpl c = cf.createConnection())
				{
					assertFalse(c.isClosed());
					
					s1.shutdown();
					
					while (dch.getCount() == 0) {
						Thread.sleep(5);
					}
					System.err.println("Proceeding");
					for (int i = 0; i < RECONNECTS-1; i++)
					{
						System.err.println("dch.get...");
						Long disconnectedAt = dch.get(5, TimeUnit.SECONDS);
						assertNotNull(disconnectedAt);
						System.err.println("rch.get...");
						long reconnectedAt = rch.get(1, TimeUnit.SECONDS);
						assertNotNull(reconnectedAt);
						long pingCycle = TimeUnit.NANOSECONDS.toMillis(disconnectedAt - reconnectedAt);
						assertFalse(pingCycle > 2 * c.opts.getPingInterval());
					}
				}
			}
		}
}
