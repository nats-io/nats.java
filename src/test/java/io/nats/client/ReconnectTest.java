/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

import static io.nats.client.Constants.*;
import static io.nats.client.UnitTestUtilities.*;

@Category(UnitTest.class)
public class ReconnectTest {
	final static Logger logger = LoggerFactory.getLogger(ReconnectTest.class);

	@Rule
	public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

	private Properties reconnectOptions = getReconnectOptions();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	private static Properties getReconnectOptions()
	{
		Properties props = new Properties();

		props.setProperty(PROP_URL,"nats://localhost:22222");
		props.setProperty(PROP_RECONNECT_ALLOWED, Boolean.toString(true));
		props.setProperty(Constants.PROP_MAX_RECONNECT, Integer.toString(10));
		props.setProperty(Constants.PROP_RECONNECT_WAIT, Integer.toString(100));

		return props;
	}

	UnitTestUtilities utils = new UnitTestUtilities();

	@Test
	public void testReconnectDisallowedFlags()
	{
		ConnectionFactory cf = new ConnectionFactory(reconnectOptions);
		cf.setUrl("nats://localhost:22222");
		cf.setReconnectAllowed(false);

		final Channel<Boolean> cch = new Channel<Boolean>();
		cf.setClosedCallback(new ClosedCallback() {
			public void onClose(ConnectionEvent event) {
				cch.add(true);
			}
		});

		try(NATSServer ts = utils.createServerOnPort(22222))
		{
			try (Connection c = cf.createConnection())
			{
				sleep(500);
				ts.shutdown();
				try {
					assertTrue(cch.get(5, TimeUnit.SECONDS));
				} catch (TimeoutException e) {
					fail(e.getMessage());
				}
			} catch (IOException | TimeoutException e1) {
				fail(e1.getMessage());
			}  
		}
	}

	@Test
	public void testReconnectAllowedFlags()
	{
		final int maxRecon = 2;
		final long reconWait = TimeUnit.SECONDS.toMillis(1);
		ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
		cf.setReconnectAllowed(true);
		cf.setMaxReconnect(maxRecon);
		cf.setReconnectWait(reconWait);

		final Channel<Boolean> cch = new Channel<Boolean>();
		final Channel<Boolean> dch = new Channel<Boolean>();

		try (NATSServer ts = utils.createServerOnPort(22222))
		{
			final AtomicLong ccbTime = new AtomicLong(0L);
			final AtomicLong dcbTime = new AtomicLong(0L);
			try (final ConnectionImpl c = cf.createConnection())
			{
				c.setClosedCallback(new ClosedCallback() {
					public void onClose(ConnectionEvent event) {
						ccbTime.set(System.nanoTime());
						long closeInterval = 
								TimeUnit.NANOSECONDS.toMillis(ccbTime.get() - dcbTime.get());
						logger.trace("in onClose(), interval was " + closeInterval);
						// Check to ensure that the disconnect cb fired first
						assertTrue ((dcbTime.get() - ccbTime.get()) < 0);
						logger.trace("Interval = " + TimeUnit.NANOSECONDS.toMillis(
								(dcbTime.get() - ccbTime.get())) + "ms");
						assertNotEquals("ClosedCB triggered prematurely.", 0L, dcbTime);
						cch.add(true);
						logger.trace("Signaled close");
						assertTrue(c.isClosed());
					}
				});

				c.setDisconnectedCallback(new DisconnectedCallback() {
					public void onDisconnect(ConnectionEvent event) {
						// Check to ensure that the closed cb didn't fire first
						dcbTime.set(System.nanoTime());
						assertTrue ((dcbTime.get() - ccbTime.get()) > 0);
						assertEquals("ClosedCB triggered prematurely.", 0, cch.getCount());
						dch.add(true);
						assertFalse(c.isClosed());
					}
				});
				assertFalse(c.isClosed());

				ts.shutdown();

				// We want wait to timeout here, and the connection
				// should not trigger the Close CB.
				assertEquals(0, cch.getCount());
				assertFalse(UnitTestUtilities.waitTime(cch, 500, TimeUnit.MILLISECONDS));


				// We should wait to get the disconnected callback to ensure
				// that we are in the process of reconnecting.
				try {
					assertTrue("DisconnectedCB should have been triggered.", 
							dch.get(5, TimeUnit.SECONDS));
					assertEquals("dch should be empty", 0, dch.getCount());
				} catch (TimeoutException e) {
					fail("DisconnectedCB triggered incorrectly. " + e.getMessage());				
				}

				assertTrue("Expected to be in a reconnecting state", c.isReconnecting());

				//				assertEquals(1, cch.getCount());
				//				assertTrue(UnitTestUtilities.waitTime(cch, 1000, TimeUnit.MILLISECONDS));

			} // Connection
			catch (IOException | TimeoutException e1) {
				fail("Should have connected OK: " + e1.getMessage());
			}
			catch (NullPointerException e) {
				e.printStackTrace();
			}
		} // NATSServer
	}

	@Test
	public void testBasicReconnectFunctionality()
	{
		ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
		cf.setMaxReconnect(2);
		cf.setReconnectWait(5000);

		final Channel<Boolean> ch = new Channel<Boolean>();
		final Channel<Boolean> dch = new Channel<Boolean>();

		cf.setDisconnectedCallback(new DisconnectedCallback() {
			public void onDisconnect(ConnectionEvent event) {
				dch.add(true);;
			}
		});
		final String testString = "bar";

		try (NATSServer ns1 = utils.createServerOnPort(22222))
		{
			try (Connection c = cf.createConnection()) {
				AsyncSubscription s = 
						c.subscribeAsync("foo", new MessageHandler() {

							@Override
							public void onMessage(Message msg) {
								String s = new String(msg.getData());
								if (!s.equals(testString))
									fail("String doesn't match");
								ch.add(true);
							}			
						});

				c.flush();

				ns1.shutdown();
				// server is stopped here...

				assertTrue("Did not get the disconnected callback on time",
						dch.get(5, TimeUnit.SECONDS));

				UnitTestUtilities.sleep(50);
				c.publish("foo", testString.getBytes());

				// restart the server.
				try (NATSServer ns2 = utils.createServerOnPort(22222))
				{
					c.flush(5000);
					assertTrue("Did not receive our message", 
							ch.get(5, TimeUnit.SECONDS));
					assertEquals("Wrong number of reconnects.", 
							1, c.getStats().getReconnects());
					c.setClosedCallback(null);
					c.setDisconnectedCallback(null);
				} // ns2
			} // Connection
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		} // ns1
	}

	@Test
	public void testExtendedReconnectFunctionality() 
	{
		Properties opts = reconnectOptions;
		ConnectionFactory cf = new ConnectionFactory(opts);

		final Channel<Boolean> dch = new Channel<Boolean>();
		cf.setDisconnectedCallback(new DisconnectedCallback() {
			public void onDisconnect(ConnectionEvent event) {
				dch.add(true);
			}
		});

		final Channel<Boolean> rch = new Channel<Boolean>();
		cf.setReconnectedCallback(new ReconnectedCallback() {
			public void onReconnect(ConnectionEvent event) {
				rch.add(true);
			}
		});

		final AtomicInteger received = new AtomicInteger(0);

		MessageHandler mh = new MessageHandler()
		{
			@Override
			public void onMessage(Message msg) {
				received.incrementAndGet();
			}
		};

		byte[] payload = "bar".getBytes();
		try (NATSServer ns = utils.createServerOnPort(22222))
		{
			try (Connection c = cf.createConnection())
			{
				c.subscribe("foo", mh);
				AsyncSubscription sub  = c.subscribe("foobar", mh);

				received.set(0);

				c.publish("foo", payload);
				try {
					c.flush();
				} catch (Exception e2) {
					fail(e2.getMessage());
				}

				ns.shutdown();
				// server is stopped here.

				// wait for disconnect
				try {
					assertTrue("Did not receive a disconnect callback message",
							dch.get(2,TimeUnit.SECONDS));
				} catch (TimeoutException e1) {
					fail("Did not receive a disconnect callback message");
				}

				// subscribe to bar while disconnected.
				c.subscribe("bar", mh);

				// Unsub foobar while disconnected
				sub.unsubscribe();

				c.publish("foo", payload);
				c.publish("bar", payload);

				// wait for reconnect
				try (NATSServer ts1 = utils.createServerOnPort(22222)) {
					// server is restarted here...

					try {
						assertTrue("Did not receive a reconnect callback message",
								rch.get(2, TimeUnit.SECONDS));
					} catch (TimeoutException e) {
						fail("Did not receive a reconnect callback message: " + e.getMessage());
					}

					c.publish("foobar", payload);
					c.publish("foo", payload);

					final Channel<Boolean> ch = new Channel<Boolean>(); 
					c.subscribe("done",new MessageHandler()
					{
						public void onMessage(Message msg)
						{
							ch.add(true);
						}
					});

					UnitTestUtilities.sleep(100);
					c.publish("done", null);

					try {
						assertTrue("Did not receive a disconnect callback message",
								ch.get(5, TimeUnit.SECONDS));
					} catch (TimeoutException e) {
						fail("Did not receive our message: "+
								e.getMessage());
					}

					UnitTestUtilities.sleep(50);

					assertEquals(4, received.get());

					c.setDisconnectedCallback(null);
				}
			} // Connection c
			catch (IOException | TimeoutException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				fail(e1.getMessage());
			}
		} // NATSServer ns
	}

	final Object mu = new Object();
	final Map<Integer,Integer> results = new ConcurrentHashMap<Integer, Integer>();

	@Test
	public void testQueueSubsOnReconnect() throws IllegalStateException, Exception
	{
		ConnectionFactory cf = new ConnectionFactory(reconnectOptions);
		try (NATSServer ts = utils.createServerOnPort(22222)) {

			final Channel<Boolean> rch = new Channel<Boolean>();
			cf.setReconnectedCallback(new ReconnectedCallback() {
				public void onReconnect(ConnectionEvent event) {
					rch.add(true);
				}
			});

			try (Connection c = cf.createConnection())
			{
				assertFalse(c.isClosed());
				// TODO create encoded connection

				// Make sure we got what we needed, 1 msg only and all seqnos accounted for..
				MessageHandler cb = new MessageHandler() {
					public void onMessage(Message msg) {
						String num = new String(msg.getData());
						Integer seqno = Integer.parseInt(num);
						if (results.containsKey(seqno))
						{
							Integer val = results.get(seqno);
							results.put(seqno, val++);
						}
						else {
							results.put(seqno, 1);
						}
					} // onMessage
				};


				String subj = "foo.bar";
				String qgroup = "workers";

				c.subscribe(subj, qgroup, cb);
				c.subscribe(subj, qgroup, cb);
				c.flush();

				//Base test
				sendAndCheckMsgs(c, subj, 10);

				// Stop and restart server
				ts.shutdown();

				// start back up
				try (NATSServer ts2 = utils.createServerOnPort(22222)) {
					sleep(500);
					assertTrue("Did not fire ReconnectedCB",
							rch.get(3,TimeUnit.SECONDS));
					sendAndCheckMsgs(c, subj, 10);
				} // ts2
			} // connection
		} // ts

	} 

	void checkResults(int numSent) {
		for (int i=0; i< numSent; i++) {
			if (results.containsKey(i))
			{
				assertEquals("results count should have been 1 for "+String.valueOf(i), 
						(int)1, (int)results.get(i));					
			} else {
				fail("results doesn't contain seqno: " + i);
			}
		}
		results.clear();
	}
	public void sendAndCheckMsgs(Connection c, String subj, int numToSend) {
		int numSent = 0;
		for (int i=0; i < numToSend; i++)
		{
			try {
				c.publish(subj, Integer.toString(i).getBytes());
			} catch (IllegalStateException e) {
				fail(e.getMessage());
			}
			numSent++;
		}
		// Wait for processing
		try {
			c.flush();
		} catch (Exception e) {
			fail(e.getMessage());
		}
		try {Thread.sleep(50);} catch(InterruptedException e) {}

		checkResults(numSent);
	}

	@Test
	public void testIsClosed() throws IOException, TimeoutException
	{
		ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
		cf.setReconnectAllowed(true);
		cf.setMaxReconnect(60);

		Connection c = null;
		try (NATSServer s1 = utils.createServerOnPort(22222)) {
			c = cf.createConnection();
			assertFalse("isClosed returned true when the connection is still open.",
					c.isClosed());
		}

		assertFalse("isClosed returned true when the connection is still open.",
				c.isClosed());

		try (NATSServer s2 = utils.createServerOnPort(22222)) {
			assertFalse("isClosed returned true when the connection is still open.",
					c.isClosed());

			c.close();
			assertTrue("isClosed returned false after close() was called.", 
					c.isClosed());
		}
	}

	@Test 
	public void testIsReconnectingAndStatus() {

		try (NATSServer ts = utils.createServerOnPort(22222)) {
			final Channel<Boolean> dch = new Channel<Boolean>();
			final Channel<Boolean> rch = new Channel<Boolean>();

			ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
			cf.setReconnectAllowed(true);
			cf.setMaxReconnect(10000);
			cf.setReconnectWait(100);

			cf.setDisconnectedCallback(new DisconnectedCallback() {
				public void onDisconnect(ConnectionEvent event) {
					dch.add(true);
				}
			});

			cf.setReconnectedCallback(new ReconnectedCallback() {
				public void onReconnect(ConnectionEvent event) {
					rch.add(true);
				}
			});

			// Connect, verify initial reconnecting state check, then stop the server
			try (Connection c = cf.createConnection())
			{
				assertFalse("isReconnecting returned true when the connection is still open.",
						c.isReconnecting());

				assertEquals(ConnState.CONNECTED, c.getState());

				ts.shutdown();

				try
				{
					assertTrue("Disconnect callback wasn't triggered.",
							dch.get(5,TimeUnit.SECONDS));
				} catch (TimeoutException e) {
					e.printStackTrace();
					fail(e.getMessage());
				} 
				assertTrue("isReconnecting returned false when the client is reconnecting.", 
						c.isReconnecting());

				assertEquals(ConnState.RECONNECTING, c.getState());

				// Wait until we get the reconnect callback
				try (NATSServer ts2 = utils.createServerOnPort(22222)) {
					try
					{
						assertTrue("reconnectedCB callback wasn't triggered.", 
								rch.get(3, TimeUnit.SECONDS));
					} catch (TimeoutException e) {
						e.printStackTrace();
						fail(e.getMessage());
					}

					assertFalse("isReconnecting returned true after the client was reconnected.", 
							c.isReconnecting());

					assertEquals(ConnState.CONNECTED, c.getState());

					// Close the connection, reconnecting should still be false
					c.close();

					assertFalse("isReconnecting returned true after close() was called.",
							c.isReconnecting());

					assertTrue("Status returned " + c.getState() +
							" after close() was called instead of CLOSED",
							c.isClosed());
				}
			} catch (IOException | TimeoutException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				fail();
			}
		}
	}

	@Test
	public void testDefaultReconnectFailure() {
		try (NATSServer ts = utils.createServerOnPort(4222)) {
			ConnectionFactory cf = new ConnectionFactory();
			cf.setMaxReconnect(4);

			final Channel<Boolean> cch = new Channel<Boolean>();
			try (Connection c = cf.createConnection())
			{
				assertFalse(c.isClosed());

				final Channel<Boolean> dch = new Channel<Boolean>();
				c.setDisconnectedCallback(new DisconnectedCallback() {
					public void onDisconnect(ConnectionEvent event) {
						dch.add(true);
					}
				});
				final Channel<Boolean> rch = new Channel<Boolean>();
				c.setReconnectedCallback(new ReconnectedCallback() {
					public void onReconnect(ConnectionEvent event) {
						rch.add(true);
					}
				});

				c.setClosedCallback(new ClosedCallback() {
					public void onClose(ConnectionEvent event) {
						cch.add(true);
					}
				});

				ts.shutdown();

				boolean disconnected = false;
				try {
					disconnected = dch.get(1,TimeUnit.SECONDS);
				} catch (TimeoutException e) {
					fail("Should have signaled disconnected");
				}
				assertTrue("Should have signaled disconnected", disconnected);

				boolean reconnected = false;
				try {
					reconnected = rch.get(1,TimeUnit.SECONDS);
				} catch (TimeoutException e) {
					// This should happen
				}
				assertFalse("Should not have reconnected", reconnected);

			} // conn
			catch (IOException | TimeoutException e1) {
				e1.printStackTrace();
				fail(e1.getMessage());
			}
			boolean closed = false;
			try {
				closed = cch.get(2, TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				fail("Connection didn't close within timeout");
			}
			assertTrue("Conn should have closed within timeout", closed);

			//			assertEquals(0, c.getStats().getReconnects());
			//			assertTrue("Should have been closed but is " + c.getState(), c.isClosed());

		} // ts
	}
}
