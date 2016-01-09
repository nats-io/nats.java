package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

import static io.nats.client.Constants.*;

@Category(UnitTest.class)
public class ReconnectTest {
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
	public void testReconnectDisallowedFlags() throws Exception
	{
		final Lock closedLock = new ReentrantLock();
		final Condition closed = closedLock.newCondition();

		ConnectionFactory cf = new ConnectionFactory(reconnectOptions);
		cf.setUrl("nats://localhost:22222");
		cf.setReconnectAllowed(false);

		cf.setClosedCallback(new ClosedCallback() {
			@Override
			public void onClose(ConnectionEvent event) {
				closedLock.lock();
				try {
					closed.signal();
				} finally {
					closedLock.unlock();
				}
			}
		});

		try(NATSServer ns = utils.createServerOnPort(22222))
		{
			try (Connection c = cf.createConnection())
			{
				Thread.sleep(500);
				closedLock.lock();
				try
				{
					ns.shutdown();
					assertTrue(closed.await(2, TimeUnit.SECONDS));
				} catch (InterruptedException e) {

				} finally {
					closedLock.unlock();
				}
			} catch (Exception e) {
				fail(e.getMessage());
			} 
		}
	}

	@Test
	public void testReconnectAllowedFlags() throws Exception
	{
		final Lock closedLock = new ReentrantLock();
		final Condition closed = closedLock.newCondition();
		final Lock disconnectedLock = new ReentrantLock();
		final Condition disconnected = disconnectedLock.newCondition();

		ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
		cf.setReconnectAllowed(true);
		cf.setMaxReconnect(2);
		cf.setReconnectWait(1000);

		cf.setClosedCallback(new ClosedCallback() {
			@Override
			public void onClose(ConnectionEvent event) {
				closedLock.lock();
				try {
					closed.signal();
				} finally {
					closedLock.unlock();
				}
			}
		});
		cf.setDisconnectedCallback(new DisconnectedCallback() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				disconnectedLock.lock();
				try
				{				
					disconnected.signal();
				} finally {
					disconnectedLock.unlock();
				}

			}
		});

		try (NATSServer ns = utils.createServerOnPort(22222))
		{
			try (Connection c = cf.createConnection())
			{
				assertFalse(c.isClosed());

				ns.shutdown();

				closedLock.lock();
				try {
					assertFalse("ClosedCB triggered incorrectly.",
							closed.await(500, TimeUnit.MILLISECONDS));
				} finally {
					closedLock.unlock();
				}

				//			ns.shutdown();

				disconnectedLock.lock();
				try {
					assertTrue("DisconnectedCB should have been triggered.", 
							disconnected.await(10, TimeUnit.SECONDS));
				} finally {
					disconnectedLock.unlock();
				}

				c.setClosedCallback(null);
			} // Connection
		} // NATSServer
	}

	@Test
	public void testBasicReconnectFunctionality()
	{
		ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
		cf.setMaxReconnect(2);
		cf.setReconnectWait(5000);

		final Lock disconnectedLock = new ReentrantLock();
		final Condition disconnected = disconnectedLock.newCondition();
		final Lock msgLock = new ReentrantLock();
		final Condition gotmessage = msgLock.newCondition();

		cf.setDisconnectedCallback(new DisconnectedCallback() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				disconnectedLock.lock();
				try {
					disconnected.signal();
				} finally {
					disconnectedLock.unlock();
				}
			}
		});

		cf.setReconnectedCallback(new ReconnectedCallback() {
			@Override
			public void onReconnect(ConnectionEvent event) {
			}
		});


		try (NATSServer ns1 = utils.createServerOnPort(22222))
		{
			try (Connection c = cf.createConnection()) {
				AsyncSubscription s = 
						c.subscribeAsync("foo", new MessageHandler() {

							@Override
							public void onMessage(Message msg) {
								msgLock.lock();
								try
								{
									gotmessage.signal();
								} finally {
									msgLock.unlock();
								}
							}			
						});

				c.flush();

				disconnectedLock.lock();
				try
				{
					ns1.shutdown();
					assertTrue("Should have received disconnect notification",
							disconnected.await(3, TimeUnit.SECONDS));
				} finally {
					disconnectedLock.unlock();
				}

				c.publish("foo", "Hello".getBytes(Charset.forName("UTF-8")));

				// restart the server.
				try (NATSServer ns2 = utils.createServerOnPort(22222))
				{
					msgLock.lock();
					try
					{
						c.flush(50000);
						assertTrue("Expected to receive message", 
								gotmessage.await(5, TimeUnit.SECONDS));
					} finally {
						msgLock.unlock();
					}
					assertEquals("Wrong number of reconnects.", 
							1, c.getStats().getReconnects());
					c.setClosedCallback(null);
					c.setDisconnectedCallback(null);
				} // ns2
			} // Connection
			catch (Exception e) {
				fail(e.getMessage());
			}
		} // ns1
	}

	AtomicInteger received = new AtomicInteger(0);

	@Test
	public void testExtendedReconnectFunctionality() 
			throws IllegalStateException, Exception
	{
		Properties opts = reconnectOptions;
		ConnectionFactory cf = new ConnectionFactory(opts);

		final Lock disconnectedLock = new ReentrantLock();
		final Condition disconnected = disconnectedLock.newCondition();
		final Lock reconnectedLock = new ReentrantLock();
		final Condition reconnected = reconnectedLock.newCondition();

		MessageHandler mh = new MessageHandler()
		{
			@Override
			public void onMessage(Message msg) {
				received.incrementAndGet();
			}
		};

		cf.setDisconnectedCallback(new DisconnectedCallback() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				disconnectedLock.lock();
				try
				{
					disconnected.signal();
				} finally {
					disconnectedLock.unlock();
				}
			}
		});

		cf.setReconnectedCallback(new ReconnectedCallback() {
			@Override
			public void onReconnect(ConnectionEvent event) {
				reconnectedLock.lock();
				try
				{
					reconnected.signal();
				} finally {
					reconnectedLock.unlock();
				}
			}
		});

		NATSServer ts = null;
		byte[] payload = "bar".getBytes();
		try (NATSServer ns = utils.createServerOnPort(22222))
		{
			try (Connection c = cf.createConnection())
			{
				AsyncSubscription s1 = c.subscribe("foo", mh);
				AsyncSubscription s2 = c.subscribe("foobar", mh);

				received.set(0);

				c.publish("foo", payload);
				c.flush();

				disconnectedLock.lock();
				try
				{
					ns.shutdown();
					// server is stopped here.
					assertTrue("Did not receive a disconnect callback message",
							disconnected.await(20,TimeUnit.SECONDS));
				} catch (InterruptedException e) {
				} finally {
					disconnectedLock.unlock();
				}

				// subscribe to bar while disconnected.
				AsyncSubscription s3 = c.subscribe("bar", mh);

				// Unsub foobar while disconnected
				s2.unsubscribe();

				c.publish("foo", payload);
				c.publish("bar", payload);

				// wait for reconnect
				reconnectedLock.lock();
				try
				{
					ts = utils.createServerOnPort(22222);
					// server is restarted here...

					assertTrue("Did not receive a reconnect callback message",
							reconnected.await(2, TimeUnit.SECONDS));
				} catch (InterruptedException e) {
				} finally {
					reconnectedLock.unlock();
				}

				c.publish("foobar", payload);
				c.publish("foo", payload);

				final Lock msgLock = new ReentrantLock();
				final Condition done = msgLock.newCondition();
				AsyncSubscription s4 = c.subscribe("done",new MessageHandler()
				{
					public void onMessage(Message msg)
					{
						msgLock.lock();
						try
						{
							done.signal();
						} finally {
							msgLock.unlock();
						}
					}
				});

				msgLock.lock();
				try
				{
					c.publish("done", payload);
					assertTrue("Did not receive a disconnect callback message",
							done.await(2, TimeUnit.SECONDS));
				} finally {
					msgLock.unlock();
				}

				assertEquals("Wrong number of messages.", 
						4, received.get());
				c.setDisconnectedCallback(null);
				ts.shutdown();
			} // Connection c
		} // NATSServer ns
	}

	final Object mu = new Object();
	final Map<Integer,Integer> results = new ConcurrentHashMap<Integer, Integer>();

	@Test
	public void testQueueSubsOnReconnect() throws IllegalStateException, Exception
	{
		ConnectionFactory cf = new ConnectionFactory(reconnectOptions);

		final Lock reconnectedLock = new ReentrantLock();
		final Condition reconnected = reconnectedLock.newCondition();

		String subj = "foo.bar";
		String qgroup = "workers";

		cf.setReconnectedCallback(new ReconnectedCallback()
		{
			@Override
			public void onReconnect(ConnectionEvent event) {
				reconnectedLock.lock();
				try
				{
					reconnected.signal();
				} finally {
					reconnectedLock.unlock();
				}
			}
		});
		Connection c = null;
		try (NATSServer ts = utils.createServerOnPort(22222)) {

			c = cf.createConnection();
			assertFalse(c.isClosed());
			// TODO create encoded connection

			// Make sure we got what we needed, 1 msg only and all seqnos accounted for..

			MessageHandler cb = new MessageHandler() {
				@Override
				public void onMessage(Message msg) {
					//				synchronized(mu) {
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

			c.queueSubscribe(subj, qgroup, cb);
			c.queueSubscribe(subj, qgroup, cb);
			c.flush();

			//Base test
			sendAndCheckMsgs(c, subj, 10);
		} // ts
		// server should stop...

		// start back up
		reconnectedLock.lock();
		try{
			try (NATSServer ts2 = utils.createServerOnPort(22222)) {

				assertTrue("Did not fire ReconnectedCB",
						reconnected.await(3,TimeUnit.SECONDS));
				sendAndCheckMsgs(c, subj, 10);
			} // ts2

		} finally {
			reconnectedLock.unlock();
		}
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

		NATSServer ts = utils.createServerOnPort(22222);

		// This will kill the last 'ts' server that is created
		//defer func() { ts.Shutdown() }()
		//disconnectedch := make(chan bool)
		final Lock disconnectedLock = new ReentrantLock(); 
		final Condition disconnected = disconnectedLock.newCondition();
		//reconnectch := make(chan bool)
		final Lock reconnectedLock = new ReentrantLock(); 
		final Condition reconnected = reconnectedLock.newCondition();

		ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
		cf.setReconnectAllowed(true);
		cf.setMaxReconnect(10000);
		cf.setReconnectWait(100);

		cf.setDisconnectedCallback(new DisconnectedCallback() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				disconnectedLock.lock();
				try
				{
					disconnected.signal();
				} finally {
					disconnectedLock.unlock();
				}
			}
		});

		cf.setReconnectedCallback(new ReconnectedCallback() {
			@Override
			public void onReconnect(ConnectionEvent event) {
				reconnectedLock.lock();
				try
				{
					reconnected.signal();
				} finally {
					reconnectedLock.unlock();
				}
			}
		});

		// Connect, verify initial reconnecting state check, then stop the server
		Connection c = null;
		try {
			c = cf.createConnection();
		} catch (Exception e) {
			ts.shutdown();
			fail("Should have connected ok: " + e.getMessage());
		}

		assertFalse("isReconnecting returned true when the connection is still open.",
				c.isReconnecting());

		assertTrue("Status returned " + c.getState() +" when connected instead of CONNECTED",
				c.getState()==ConnState.CONNECTED);

		ts.shutdown();

		disconnectedLock.lock();
		try
		{
			assertTrue("Disconnect callback wasn't triggered.",
					disconnected.await(3,TimeUnit.SECONDS));
		} catch (InterruptedException e) {
		} finally {
			disconnectedLock.unlock();
		}

		assertTrue("isReconnecting returned false when the client is reconnecting.", 
				c.isReconnecting());

		assertTrue("Status returned " + c.getState() +" when connected instead of RECONNECTING",
				c.getState()==ConnState.RECONNECTING);

		// Wait until we get the reconnect callback
		reconnectedLock.lock();
		try 
		{
			ts = utils.createServerOnPort(22222);

			assertTrue("reconnectedCB callback wasn't triggered.", 
					reconnected.await(3, TimeUnit.SECONDS));
		} catch (InterruptedException e) {
		} finally {
			reconnectedLock.lock();
		}

		assertFalse("isReconnecting returned true after the client was reconnected.", c.isReconnecting());

		assertTrue("Status returned " + c.getState() +" when reconnected instead of CONNECTED",
				c.getState()==ConnState.CONNECTED);

		// Close the connection, reconnecting should still be false
		c.close();

		assertFalse("isReconnecting returned true after close() was called.",
				c.isReconnecting());

		assertTrue("Status returned " + c.getState() +
				" after close() was called instead of CLOSED",
				c.getState()==ConnState.CLOSED);

		ts.shutdown();
	}
	@Test
	public void testDefaultReconnectFailure() {
		final Lock reconnectLock = new ReentrantLock();
		final Condition reconnected = reconnectLock.newCondition();

		UnitTestUtilities.startDefaultServer();
		ConnectionFactory cf = new ConnectionFactory();
		cf.setMaxReconnect(4);
		try (Connection c = cf.createConnection())
		{
			assertFalse(c.isClosed());
			c.setReconnectedCallback(new ReconnectedCallback() {

				@Override
				public void onReconnect(ConnectionEvent event) {
					reconnectLock.lock();
					try {
						reconnected.signal();
					} finally {
						reconnectLock.unlock();
					}
				}
			});

			try {Thread.sleep(500);} catch (InterruptedException e) {}
			reconnectLock.lock();
			try {
				UnitTestUtilities.stopDefaultServer();
				assertFalse(reconnected.await(3,TimeUnit.SECONDS));
				while(c.isReconnecting())
					Thread.sleep(500);
				assertTrue("Should have been closed but is " + c.getState(), c.isClosed());
			} catch (InterruptedException e) {
			} finally {
				reconnectLock.unlock();
				UnitTestUtilities.stopDefaultServer();
			}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}
}
