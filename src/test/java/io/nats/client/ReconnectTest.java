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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import io.nats.client.Connection.ConnState;

import static io.nats.client.Constants.*;

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
		final Object testLock = new Object();
		final AtomicBoolean closed = new AtomicBoolean(false);

		ConnectionFactory cf = new ConnectionFactory(getReconnectOptions());
		cf.setUrl("nats://localhost:22222");
		cf.setReconnectAllowed(false);

		cf.setClosedEventHandler(new ClosedEventHandler() {
			@Override
			public void onClose(ConnectionEvent event) {
				synchronized(testLock) {
					System.out.println("Signaling closed condition");
					closed.set(true);
					testLock.notify();
				}
				System.err.println("In onClose(), done notifying");
			}
		});

		NATSServer ns = utils.createServerOnPort(22222);
		//		try { Thread.sleep(500); } catch (InterruptedException e) {}
		Connection c = null;

		try {
			c = cf.createConnection();
		} catch (Exception e) {
			throw e;
		} finally {
			if (c != null)
				c.close();
		}

		synchronized(testLock)
		{
			ns.shutdown();
			try {
				testLock.wait(1000);
				assertTrue(closed.get());
				System.err.println("closed is true");
			} catch (InterruptedException e) {}
		}
	}

	@Test
	public void testReconnectAllowedFlags() throws Exception
	{
		final AtomicBoolean closed = new AtomicBoolean(false);
		final Object closedLock = new Object();
		final AtomicBoolean disconnected = new AtomicBoolean(false);
		final Object disconnectedLock = new Object();

		ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
		cf.setReconnectAllowed(true);
		cf.setMaxReconnect(2);
		cf.setReconnectWait(1000);

		cf.setClosedEventHandler(new ClosedEventHandler() {
			@Override
			public void onClose(ConnectionEvent event) {
				synchronized(closedLock) {
					System.out.println("Signaling closed condition");
					closed.set(true);
					closedLock.notify();
				}
			}
		});
		cf.setDisconnectedEventHandler(new DisconnectedEventHandler() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				synchronized(disconnectedLock)
				{				
					System.out.println("Signaling disconnected condition");
					disconnected.set(true);
					disconnectedLock.notify();
				}
			}
		});

		NATSServer ns = utils.createServerOnPort(22222);
		Connection c = null;
		try {
			System.err.println("Connecting");
			c = cf.createConnection();
			assertTrue(c !=null && !c.isClosed());
			System.err.println("Connected");
		} catch (Exception e) {
			throw e;
		} finally {
		}

		ns.shutdown();

		System.out.println("Waiting for closed condition");
		synchronized (closedLock) {
			closedLock.wait(500);
			assertFalse("ClosedCB triggered incorrectly.", 
					closed.get());
		}

		System.out.println("waiting on disconnected condition");
		//			ns.shutdown();

		synchronized (disconnectedLock) {
			disconnectedLock.wait(10000);
			assertTrue("DisconnectedCB should have been triggered.", 
					disconnected.get());
		}

		c.setClosedEventHandler(null);
		c.close();
	}

	@Test
	public void testBasicReconnectFunctionality() throws Exception
	{
		ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
		cf.setMaxReconnect(2);
		cf.setReconnectWait(5000);

		final AtomicBoolean disconnected = new AtomicBoolean(false);
		final AtomicBoolean gotmessage = new AtomicBoolean(false);
		final Object testLock = new Object();
		final Object msgLock = new Object();

		cf.setDisconnectedEventHandler(new DisconnectedEventHandler() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				System.err.println("Disconnected");
				synchronized(testLock) {
					disconnected.set(true);
					testLock.notify();
				}
			}
		});

		cf.setReconnectedEventHandler(new ReconnectedEventHandler() {

			@Override
			public void onReconnect(ConnectionEvent event) {
				System.err.println("Reconnected");
			}
		});


		NATSServer ns = utils.createServerOnPort(22222);

		Connection c = cf.createConnection();
		AsyncSubscription s = c.subscribeAsync("foo");
		s.setMessageHandler(new MessageHandler() {

			@Override
			public void onMessage(Message msg) {
				System.out.println("Received message.");
				synchronized(msgLock)
				{
					gotmessage.set(true);
					msgLock.notify();  
				}

			}			
		});

		s.start();
		c.flush();

		synchronized(testLock)
		{
			System.err.println("Waiting for disconnect notification");
			ns.shutdown();
			testLock.wait(100000);
			assertTrue("Should have received disconnect notification", disconnected.get());
		}

		System.out.println("Sending message.");
		c.publish("foo", "Hello".getBytes(Charset.forName("UTF-8")));
		System.out.println("Done sending message.");
		// restart the server.
		ns = utils.createServerOnPort(22222);
		synchronized(msgLock)
		{
			c.flush(50000);
			msgLock.wait(10000);
			assertTrue("Expected to receive message", gotmessage.get());
		}

		assertTrue("stats.reconnects was " + c.getStats().getReconnects() 
				+ ". Expected 1", c.getStats().getReconnects() == 1);
		ns.shutdown();
		c.setClosedEventHandler(null);
		c.close();
	}

	AtomicInteger received = new AtomicInteger(0);

	@Test
	public void testExtendedReconnectFunctionality() throws IllegalStateException, Exception
	{
		Properties opts = getReconnectOptions();
		ConnectionFactory cf = new ConnectionFactory(opts);

		final Object disconnectedLock = new Object();
		final AtomicBoolean disconnected = new AtomicBoolean(false);
		final Object msgLock = new Object();
		final Object reconnectedLock = new Object();
		final AtomicBoolean reconnected = new AtomicBoolean(false);
		MessageHandler mh = new IncrementReceivedMessageHandler();

		cf.setDisconnectedEventHandler(new DisconnectedEventHandler() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				System.out.println("Disconnected.");
				synchronized(disconnectedLock)
				{
					disconnected.set(true);
					disconnectedLock.notify();
				}
			}
		});
		
		cf.setReconnectedEventHandler(new ReconnectedEventHandler() {
			@Override
			public void onReconnect(ConnectionEvent event) {
				System.out.println("Reconnected.");
				synchronized(reconnectedLock)
				{
					reconnected.set(true);
					reconnectedLock.notify();
				}				
			}
		});

		byte[] payload = "bar".getBytes(Charset.forName("UTF-8"));
		NATSServer ns = utils.createServerOnPort(22222);

		Connection c = cf.createConnection();
		AsyncSubscription s1 = c.subscribe("foo", mh);
		AsyncSubscription s2 = c.subscribe("foobar", mh);

		received.set(0);

		c.publish("foo", payload);
		c.flush();

		synchronized(disconnectedLock)
		{
			ns.shutdown();
			// server is stopped here.
			try {
				disconnectedLock.wait(20000);
			} catch (InterruptedException e) {
			}
			assertTrue("Did not receive a disconnect callback message",
					disconnected.get());
		}

		// subscribe to bar while disconnected.
		AsyncSubscription s3 = c.subscribe("bar", mh);

		// Unsub foobar while disconnected
		s2.unsubscribe();

		try {
			c.publish("foo", payload);
			c.publish("bar", payload);
		} catch (Exception e) {
			fail("Received an error after disconnect: "+ e.getMessage());
		}

		// server is restarted here...
		NATSServer ts = utils.createServerOnPort(22222);
		// wait for reconnect
		synchronized(reconnectedLock)
		{
			try {
				reconnectedLock.wait(2000);
			} catch (InterruptedException e) {
			}
			assertTrue("Did not receive a reconnect callback message",
					reconnected.get());
		}

		try {
			c.publish("foobar", payload);
			c.publish("foo", payload);
		} catch (Exception e) {
			fail("Received an error after server restarted: "+ e.getMessage());
		}

		final Object doneLock = new Object();
		final AtomicBoolean done = new AtomicBoolean(false);
		AsyncSubscription s4 = c.subscribe("done",new MessageHandler()
		{
			public void onMessage(Message msg)
			{
				System.err.println("Received done message.");
				synchronized(doneLock)
				{
					done.set(true);
					doneLock.notify();
				}
			}
		});

		synchronized(doneLock)
		{
			c.publish("done", payload);
			try {doneLock.wait(2000);} catch (InterruptedException e) {}
			assertTrue("Did not receive a disconnect callback message",
					done.get());
		}

		assertTrue("Expected 4, received " + received.get(), 
				received.get()==4);

		c.close();
		ts.shutdown();
	}

	class IncrementReceivedMessageHandler implements MessageHandler
	{
		@Override
		public void onMessage(Message msg) {
			System.out.println("Received message on subject "+
					msg.getSubject() + ".");
			received.incrementAndGet();
		}
	}

	final Object mu = new Object();
	final Map<Integer,Integer> results = new ConcurrentHashMap<Integer, Integer>();

	@Test
	public void testQueueSubsOnReconnect() throws IllegalStateException, Exception
	{
		NATSServer ts = utils.createServerOnPort(22222);
		ConnectionFactory cf = new ConnectionFactory(getReconnectOptions());

		final AtomicBoolean reconnected = new AtomicBoolean(false);
		final Object reconnectedLock = new Object();

		cf.setReconnectedEventHandler(new ReconnectedEventHandler()
		{
			@Override
			public void onReconnect(ConnectionEvent event) {
				System.out.println("Reconnected.");
				synchronized(reconnectedLock)
				{
					reconnected.set(true);
					reconnectedLock.notify();
				}				
			}
		});

		Connection c = null;
		try {
			c = cf.createConnection();
			assertFalse(c.isClosed());
		} catch (IOException e) {
			fail("Should have connected ok: " + e.getMessage());
		} catch (TimeoutException e) {
			fail("Should have connected ok: " + e.getMessage());
		}

		// TODO create encoded connection
		//		Connection c = cf.createConnection();


		// Make sure we got what we needed, 1 msg only and all seqnos accounted for..

		String subj = "foo.bar";
		String qgroup = "workers";

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
				//				} // mu
			} // onMessage
		};

		c.QueueSubscribe(subj, qgroup, cb);
		c.QueueSubscribe(subj, qgroup, cb);
		c.flush();

		//Base test
		sendAndCheckMsgs(c, subj, 10);

		// Stop and restart server
		ts.shutdown();
		ts = utils.createServerOnPort(22222);

		synchronized(reconnectedLock) {
			try { reconnectedLock.wait(3000); } catch (InterruptedException e) {}
			assertTrue("Did not get the ReconnectedCB!", reconnected.get());
		}

		sendAndCheckMsgs(c, subj, 10);
		c.close();
		ts.shutdown();
	}

	void checkResults(int numSent) {
		// checkResults
		//		synchronized(mu) {
		for (int i=0; i< numSent; i++) {
			//				String errmsg = String.format(
			//						"Received incorrect number of messages, [%d] for seq: %d", 
			//						results.get(i), i);

			if (results.containsKey(i))
			{
				assertTrue("results count should have been 1 for "+i, results.get(i)==1);					
			} else {
				fail("results doesn't contain seqno: " + i);
			}
		}
		results.clear();
		//		}
	}
	public void sendAndCheckMsgs(Connection c, String subj, int numToSend) {
		int numSent = 0;
		for (int i=0; i < numToSend; i++)
		{
			c.publish(subj, Integer.toString(i).getBytes());
			numSent++;
		}
		// Wait for processing
		try {
			c.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {Thread.sleep(50);} catch(InterruptedException e) {}

		checkResults(numSent);

	}

	@Test
	public void TestIsClosed() throws IOException, TimeoutException
	{
		ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
		cf.setReconnectAllowed(true);
		cf.setMaxReconnect(60);

		NATSServer s1 = utils.createServerOnPort(22222);
		Connection c = cf.createConnection();
		assertFalse("isClosed returned true when the connection is still open.",
				c.isClosed());

		s1.shutdown();

		// FIXME - .NET says still reconnecting.
		//				Thread.sleep(100);
		assertFalse("isClosed returned true when the connection is still open.",
				c.isClosed());

		NATSServer s2 = utils.createServerOnPort(22222);
		assertFalse("isClosed returned true when the connection is still open.",
				c.isClosed());

		c.close();
		assertTrue("isClosed returned false after close() was called.", c.isClosed());
		s1.shutdown();
		s2.shutdown();
	}

	@Test 
	public void testIsReconnectingAndStatus() {

		NATSServer ts = utils.createServerOnPort(22222);

		// This will kill the last 'ts' server that is created
		//defer func() { ts.Shutdown() }()
		//disconnectedch := make(chan bool)
		final AtomicBoolean disconnected = new AtomicBoolean(false);
		final Object disconnectedLock = new Object(); 
		//reconnectch := make(chan bool)
		final AtomicBoolean reconnected = new AtomicBoolean(false);
		final Object reconnectedLock = new Object(); 

		ConnectionFactory cf = new ConnectionFactory("nats://localhost:22222");
		cf.setReconnectAllowed(true);
		cf.setMaxReconnect(10000);
		cf.setReconnectWait(100);

		cf.setDisconnectedEventHandler(new DisconnectedEventHandler() {
			@Override
			public void onDisconnect(ConnectionEvent event) {
				System.out.println("Disconnected.");
				synchronized(disconnectedLock)
				{
					disconnected.set(true);
					disconnectedLock.notify();
				}
			}
		});

		cf.setReconnectedEventHandler(new ReconnectedEventHandler() {
			@Override
			public void onReconnect(ConnectionEvent event) {
				System.out.println("Reconnected.");
				synchronized(reconnectedLock)
				{
					reconnected.set(true);
					reconnectedLock.notify();
				}				
				System.out.println("Reconnected notified.");
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

		synchronized(disconnectedLock) {
			try {disconnectedLock.wait();} catch (InterruptedException e) {}
			assertTrue("Disconnect callback wasn't triggered.", disconnected.get());
			System.err.println("DisconnectedCB triggered");
		}

		assertTrue("isReconnecting returned false when the client is reconnecting.", c.isReconnecting());

		assertTrue("Status returned " + c.getState() +" when connected instead of RECONNECTING",
				c.getState()==ConnState.RECONNECTING);

		// Wait until we get the reconnect callback
		synchronized(reconnectedLock) {
			ts = utils.createServerOnPort(22222);

			System.err.println("Waiting for reconnectedCB");
			try {reconnectedLock.wait();} catch (InterruptedException e) {}
			assertTrue("reconnectedCB callback wasn't triggered.", reconnected.get());
			System.err.println("ReconnectedCB triggered");
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

}
