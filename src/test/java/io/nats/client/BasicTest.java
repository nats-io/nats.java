/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.nats.client.Constants.*;
import static io.nats.client.UnitTestUtilities.*;

@Category(UnitTest.class)
public class BasicTest {
	@Rule
	public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

	ExecutorService executor = Executors.newFixedThreadPool(5);
	UnitTestUtilities utils = new UnitTestUtilities();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		UnitTestUtilities.startDefaultServer();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		UnitTestUtilities.stopDefaultServer();
	}

	@Before
	public void setUp() throws Exception {
		//		utils.startDefaultServer();
	}

	@After
	public void tearDown() throws Exception {
		//		utils.stopDefaultServer();
	}

	@Test
	public void testConnectedServer() throws IOException, TimeoutException
	{

		try (ConnectionImpl c = new ConnectionFactory().createConnection())
		{
			String u=null, srv=null;
			String badUrl = String.format("Unexpected connected URL of %s\n", u);

			u = c.getConnectedUrl();
			assertNotNull(badUrl, u);
			assertEquals(badUrl, ConnectionFactory.DEFAULT_URL, u);

			assertNotNull(c.currentServer().toString());
			assertTrue(c.currentServer().toString().contains(ConnectionFactory.DEFAULT_URL));

			srv = c.getConnectedServerId();
			assertNotNull("Expected a connected server id", srv); 

			c.close();
			u = c.getConnectedUrl();
			srv = c.getConnectedServerId();
			assertNull(u);
			assertNull(srv);
		}

	}

	@Test
	public void testMultipleClose()
	{
		try (final Connection c = new ConnectionFactory().createConnection())
		{

			List<Callable<String>> callables = new ArrayList<Callable<String>>(10);

			for (int i = 0; i < 10; i++)
			{   
				final int index = i;
				callables.add(new Callable<String>(){
					public String call() throws Exception {
						c.close();
						return "Task "+ index;
					}
				});			
			}
			try {
				List<Future<String>> futures = executor.invokeAll(callables);
				//				for(Future<String> future : futures){
				//				    try {
				//						System.err.println("future.get = " + future.get());
				//					} catch (ExecutionException e) {
				//						// TODO Auto-generated catch block
				//						e.printStackTrace();
				//					}
				//				}
			} catch (InterruptedException e) {
			}
		} catch (IOException | TimeoutException e1) {
			fail("Didn't connect: " + e1.getMessage());
		}
	}

	@Test(expected=IllegalArgumentException.class)
	public void testBadOptionTimeoutConnect()
	{
		ConnectionFactory cf = new ConnectionFactory();
		cf.setConnectionTimeout(-1);
	}

	@Test
	public void testSimplePublish() 
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			c.publish("foo", "Hello World".getBytes());
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testSimplePublishNoData()
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			c.publish("foo", null);
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test(expected=NullPointerException.class)
	public void testSimplePublishError() 
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			c.publish(null, "Hello World!".getBytes());
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testPublishDoesNotFailOnSlowConsumer() {
		try (Connection nc = new ConnectionFactory().createConnection())
		{
			Subscription sub = nc.subscribeSync("foo");
			sub.setMaxPendingMsgs(1);
			sub.setMaxPendingBytes(1000);
			
			byte[] msg = "Hello".getBytes();
			for (int i=0; i<10; i++) {
				try {
					nc.publish("foo", msg);
				} catch (IOException e) {
					e.printStackTrace();
					fail("publish() should not fail because of slow consumer. Got " + e.getMessage());
				}
				nc.flush();
			}
			// Make sure we did record the error, regardless
			assertTrue(nc.getLastException() instanceof IOException);
			assertEquals(ERR_SLOW_CONSUMER, nc.getLastException().getMessage());

		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		} catch (Exception e) {
			fail("Unexpected exception: " + e.getMessage());
		}
	}

	private boolean compare(byte[] p1, byte[] p2)
	{
		return Arrays.equals(p1, p2);
	}

	@Test
	public void testAsyncSubscribe()
	{
		final byte[] omsg = "Hello World".getBytes();

		final Channel<Boolean> ch = new Channel<Boolean>();
		try (Connection c = new ConnectionFactory().createConnection())
		{
			try (AsyncSubscription s = c.subscribe("foo", new MessageHandler() {
				@Override
				public void onMessage(Message msg) {
					assertArrayEquals("Message received does not match", omsg, msg.getData());

					assertNotNull("Callback does not have a valid Subscription", 
							msg.getSubscription());
					ch.add(true);
				}

			})) { 

				c.publish("foo", omsg);
				try { c.flush(); } catch (Exception e) {}
				assertTrue("Did not receive message.", ch.get(5, TimeUnit.SECONDS));
			} // AsyncSubscription
		} // Connection
		catch (IOException | TimeoutException e) {
			fail("Didn't connect: " + e.getMessage());
		}
	}

	@Test
	public void testSyncSubscribe()
	{
		final byte[] omsg = "Hello World".getBytes();
		int timeoutMsec = 1000;

		try (Connection c = new ConnectionFactory().createConnection())
		{
			try (SyncSubscription s = c.subscribeSync("foo")) {
				try { Thread.sleep(100); } catch (InterruptedException e) {}
				c.publish("foo", omsg);
				try {
					Message m = s.nextMessage(timeoutMsec);
					assertArrayEquals("Messages are not equal.", omsg, m.getData());
				} catch (IOException | TimeoutException e) {
					fail("nextMessage(timeout) failed: " + e.getMessage());
				}
			}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testPubSubWithReply()
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			try (SyncSubscription s = c.subscribeSync("foo")) {
				final byte[] omsg = "Hello World".getBytes();
				c.publish("foo", "reply", omsg);
				try { c.flush(); } catch (Exception e) { e.printStackTrace(); }
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
				Message m = s.nextMessage(10000);
				assertArrayEquals("Message received does not match: ", 
						omsg, m.getData());
			}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testFlush()
	{
		final byte[] omsg = "Hello World".getBytes();

		try (Connection c = new ConnectionFactory().createConnection())
		{
			c.subscribeSync("foo");
			c.publish("foo", "reply", omsg);
			try {
				c.flush();
			} catch (Exception e) {
				fail("Received error from flush: " + e.getMessage());
			}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testQueueSubscriber()
	{
		final byte[] omsg = "Hello World".getBytes();
		try (Connection c = new ConnectionFactory().createConnection())
		{
			SyncSubscription s1 = c.subscribeSync("foo", "bar"),
					s2 = c.subscribeSync("foo", "bar");
			c.publish("foo", omsg);
			try {c.flush();} catch (Exception e) { /* IGNORE */ }

			int r1 = s1.getQueuedMessageCount(); 
			int r2 = s2.getQueuedMessageCount();
			assertEquals("Received too many messages for multiple queue subscribers", 
					1, r1+r2);

			// Drain the messages.
			try { s1.nextMessage(1000); } catch (TimeoutException e) {} 
			assertEquals(0, s1.getQueuedMessageCount());
			try { s2.nextMessage(1000); } catch (TimeoutException e) {} 
			assertEquals(0, s2.getQueuedMessageCount());

			int total = 1000;
			for (int i = 0; i < total; i++)
			{
				c.publish("foo", omsg);
			}
			try {
				c.flush();
			} catch (Exception e) {}

			int v = (int)(total * 0.15);
			r1 = s1.getQueuedMessageCount();
			r2 = s2.getQueuedMessageCount();
			assertEquals("Incorrect number of messages: ", total, r1+r2);

			double expected = total/2;
			int d1 = (int)Math.abs((expected - r1));
			int d2 = (int)Math.abs((expected - r2));
			if (d1 > v || d2 > v) {
				fail(String.format("Too much variance in totals: %d, %d > %d", r1, r2, v));
			}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testReplyArg()
	{
		final String replyExpected = "bar";
		final String ts;

		final Channel<Boolean> ch = new Channel<Boolean>();
		try (Connection c = new ConnectionFactory().createConnection())
		{
			try (AsyncSubscription s = c.subscribeAsync("foo", new MessageHandler() {
				@Override
				public void onMessage(Message msg) {
					assertEquals(replyExpected, msg.getReplyTo());
					ch.add(true);
				}
			})) 
			{
				try {Thread.sleep(200);} catch (InterruptedException e) {}
				c.publish("foo", "bar", (byte[])null);
				assertTrue("Message not received.", ch.get(5, TimeUnit.SECONDS));
			}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testSyncReplyArg()
	{
		String replyExpected = "bar";
		try (Connection c = new ConnectionFactory().createConnection()) {
			try (SyncSubscription s = c.subscribeSync("foo")) {
				try {Thread.sleep(500);} catch (InterruptedException e) {}
				c.publish("foo", replyExpected, (byte[])null);
				Message m = null;
				try {
					m = s.nextMessage(1000);
				} catch (Exception e) {
					fail("Received an err on nextMsg(): " + e.getMessage());
				}
				assertEquals(replyExpected, m.getReplyTo());
			}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testUnsubscribe() throws Exception
	{
		final Channel<Boolean> ch = new Channel<Boolean>();
		final AtomicInteger count = new AtomicInteger(0);
		final int max = 20;
		ConnectionFactory cf = new ConnectionFactory();
		cf.setReconnectAllowed(false);
		try (Connection c = cf.createConnection()) {
			try (final AsyncSubscription s = c.subscribeAsync("foo", new MessageHandler() {
				@Override
				public void onMessage(Message m) {
					count.incrementAndGet();
					if (count.get() == max)
					{
						try {
							m.getSubscription().unsubscribe();
							assertFalse(m.getSubscription().isValid());
						} catch (Exception e) {
							fail("Unsubscribe failed with err: " + e.getMessage());
						}
						ch.add(true);
					}
				}
			}))
			{
				for (int i = 0; i < max; i++)
				{
					c.publish("foo", null, (byte[])null);
				}
				Thread.sleep(100);
				c.flush();

				if (s.isValid())
				{
					assertTrue("Test complete signal not received", ch.get(5, TimeUnit.SECONDS));
					assertFalse(s.isValid());
				}
				assertEquals(max, count.get());
			}
		}
	}

	@Test(expected=IllegalStateException.class)
	public void testDoubleUnsubscribe() 
	{
		try (Connection c = new ConnectionFactory().createConnection()) {
			try (SyncSubscription s = c.subscribeSync("foo")) {
				s.unsubscribe();
				try {
					s.unsubscribe();
				} catch (IllegalStateException e) {
					throw e;
				}
			}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test(expected=TimeoutException.class)
	public void testRequestTimeout() throws TimeoutException
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			assertFalse(c.isClosed());
			assertNull("timeout waiting for response", c.request("foo", "help".getBytes(), 10));
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}

	@Test(expected=IllegalArgumentException.class)
	public void testRequestTimeoutNegative()
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			c.request("foo", "help".getBytes(), -50);
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());		
		}
	}

	@Test(expected=IOException.class)
	public void testRequestThrowsIOEx() throws Exception
	{
		try (ConnectionImpl c = new ConnectionFactory().createConnection())
		{
			String str = String.format(ConnectionImpl.UNSUB_PROTO, 1, 1);
			byte[] unsub = str.getBytes();

			BufferedOutputStream bw = mock(BufferedOutputStream.class);
			doThrow(new IOException("Mock OutputStream write exception")).when(bw)
			.write(unsub);
			c.setOutputStream(bw);

			c.request("foo", "help".getBytes());
			
		} catch (IOException | TimeoutException e) {
			assertTrue("Wrong exception thrown", e instanceof IOException);
			assertEquals("Mock OutputStream write exception", e.getMessage());
			throw e;
		}
	}

	@Test
	public void testRequest()
	{
		final byte[] response = 
				"I will help you.".getBytes();
		try (final Connection c = new ConnectionFactory().createConnection())
		{
			UnitTestUtilities.sleep(100);
			try (AsyncSubscription s = c.subscribeAsync("foo",  
					new MessageHandler() {
				public void onMessage(Message m)
				{
					try {
						c.publish(m.getReplyTo(), response);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}))
			{
				UnitTestUtilities.sleep(100);
				final byte[] request = "help".getBytes();
				Message m = null;
				try { m = c.request("foo", request, 5000); } 
				catch (Exception e) {
					fail("Request failed: " + e.getMessage());
				}
				assertNotNull(m);
				assertArrayEquals("Received invalid response", response, m.getData());

				m = null;
				try {
					m = c.request("foo", request);
				}
				catch (Exception e) {
					fail("Request failed: " + e.getMessage());
				}
				assertArrayEquals("Response isn't valid.", response, m.getData());
			}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testRequestNoBody()
	{
		final byte[] response = "I will help you.".getBytes();

		try (final Connection c = new ConnectionFactory().createConnection()) {
			try (AsyncSubscription s = c.subscribeAsync("foo",  
					new MessageHandler() {
				public void onMessage(Message m)
				{
					try {
						c.publish(m.getReplyTo(), response);
					} catch (Exception e){}
				}
			}))
			{
				UnitTestUtilities.sleep(100);
				Message m = c.request("foo", null, 5000);
				assertArrayEquals("Response isn't valid.", response, m.getData());

			} catch (TimeoutException | IOException e) {
				fail(e.getMessage());
			}
		} catch (IOException | TimeoutException e1) {
			fail(e1.getMessage());
		}
	}

	@Test
	public void testFlushInHandler()
	{
		final Channel<Boolean> ch = new Channel<Boolean>();
		try (Connection c = new ConnectionFactory().createConnection()) {
			try (AsyncSubscription s = c.subscribeAsync("foo", new MessageHandler() {
				@Override
				public void onMessage(Message msg) {
					try { c.flush(); } catch (Exception e1) { e1.printStackTrace(); }
					ch.add(true);
				}
			}))
			{
				c.publish("foo", "Hello".getBytes());
				assertTrue("Flush did not return properly in callback", ch.get(5000));
			}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testReleaseFlush()
	{
		try (final Connection c = new ConnectionFactory().createConnection())
		{
			byte[] data = "Hello".getBytes(Charset.forName("UTF-8"));
			for (int i = 0; i < 1000; i++)
			{
				c.publish("foo", data);
			}

			executor.execute(new Runnable() {
				public void run() {
					c.close();
					assertTrue(c.isClosed());
				}
			});
			try { c.flush(); } catch (Exception e) {}
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testCloseAndDispose()
	{
		try (Connection c = new ConnectionFactory().createConnection()) {
			c.close();
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testInbox()
	{
		try (Connection c = new ConnectionFactory().createConnection()) {
			String inbox = c.newInbox();
			assertFalse("inbox was null or whitespace", 
					inbox.equals(null) || inbox.trim().length()==0);
			assertTrue("Bad INBOX format", inbox.startsWith("_INBOX."));
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testStats()
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			byte[] data = 
					"The quick brown fox jumped over the lazy dog".getBytes();
			int iter = 10;

			for (int i = 0; i < iter; i++)
			{
				c.publish("foo", data);
			}

			Statistics stats = c.getStats();
			assertEquals("Not properly tracking OutMsgs: ", 
					iter, stats.getOutMsgs());
			assertEquals("Not properly tracking OutBytes: ", 
					iter * data.length, stats.getOutBytes());

			c.resetStats();

			// Test both sync and async versions of subscribe.
			try (AsyncSubscription s1 = c.subscribe("foo", new MessageHandler() {
				public void onMessage(Message msg) {}
			}))
			{
				try (SyncSubscription s2 = c.subscribeSync("foo")) {
					for (int i = 0; i < iter; i++)
					{
						c.publish("foo", data);
					}
					try { c.flush(); } catch (Exception e) {}

					stats = c.getStats();
					String toStringOutput = stats.toString();
//					String expected = String.format("{in: msgs=%d, bytes=%d, out: msgs=%d, bytes=%d, reconnects: %d, flushes: %d}",
//							stats.getInMsgs(), stats.getInBytes(), stats.getOutMsgs(), stats.getOutBytes(),
//							stats.getReconnects(),
//							stats.getFlushes());
//					assertEquals(expected, toStringOutput);
//					System.err.printf("Stats: %s\n", stats);
					assertEquals("Not properly tracking InMsgs: ", 2 * iter, stats.getInMsgs());
					assertEquals("Not properly tracking InBytes: ", 2 * iter * data.length, stats.getInBytes());
				}
			}
		} catch (IOException | TimeoutException e1) {
			fail(e1.getMessage());
		}
	}

	@Test
	public void testRaceSafeStats()
	{
		try (Connection c = new ConnectionFactory().createConnection()) {

			//			new Task(() => { c.publish("foo", null); }).Start();
			executor.execute(new Runnable() {

				@Override
				public void run() {
					try {
						c.publish("foo", null);
					} catch (IllegalStateException | IOException e) {
						fail(e.getMessage());
					} 
				}

			});
			try {Thread.sleep(1000);} catch (InterruptedException e) {}

			assertEquals(1, c.getStats().getOutMsgs());
		} catch (IOException | TimeoutException e1) {
			fail(e1.getMessage());
		}
	}

	@Test
	public void testStatsClone() {
		Statistics s1 = new Statistics();
		Statistics s2 = null;

		s1.incrementInMsgs();
		s1.incrementInBytes(8192);
		s1.incrementOutMsgs();
		s1.incrementOutBytes(512);
		s1.incrementReconnects();

		try {
			s2 = (Statistics)s1.clone();
		} catch (CloneNotSupportedException e) {
			fail("Clone should not throw an exception");
		}
		assertEquals(s1.getInMsgs(), s2.getInMsgs());
		assertEquals(s1.getOutMsgs(), s2.getOutMsgs());
		assertEquals(s1.getInBytes(), s2.getInBytes());
		assertEquals(s1.getOutBytes(), s2.getOutBytes());
		assertEquals(s1.getReconnects(), s2.getReconnects());
	}

	@Test
	public void testBadSubject() throws IOException, TimeoutException
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
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
	public void testLargeMessage()
	{
		try (final Connection c = new ConnectionFactory().createConnection()) {
			int msgSize = 51200;
			final byte[] omsg = new byte[msgSize];
			byte[] output = null;
			for (int i = 0; i < msgSize; i++)
				omsg[i] = (byte)'A';

			omsg[msgSize-1] = (byte)'Z';

			final Channel<Boolean> ch = new Channel<Boolean>();
			AsyncSubscription s = c.subscribeAsync("foo", new MessageHandler() {
				@Override
				public void onMessage(Message msg) {
					assertTrue("Response isn't valid.", compare(omsg, msg.getData()));
					ch.add(true);
				}
			});

			c.publish("foo", omsg);
			try {
				c.flush(1000);
			} catch (Exception e1) {
				e1.printStackTrace();
				fail("Flush failed");
			}
			assertTrue("Didn't receive callback message", ch.get(2, TimeUnit.SECONDS));

		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSendAndRecv() throws Exception
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			assertFalse(c.isClosed());
			final AtomicInteger received = new AtomicInteger();
			int count = 1000;
			try (AsyncSubscription s = c.subscribeAsync("foo",
					new MessageHandler() {
				public void onMessage(Message msg) {
					received.incrementAndGet();
				}
			})) 
			{
//				s.start();
				assertFalse(c.isClosed());
				for (int i = 0; i < count; i++)
				{
					c.publish("foo", null);
				}
				c.flush();

				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {}

				assertTrue(
						String.format("Received (%s) != count (%s)", received, count), 
						received.get() == count);
			}
		}
	}

	@Test
	public void testLargeSubjectAndReply()
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			int size = 1066;
			byte[] subjBytes = new byte[size];
			for (int i = 0; i < size; i++)
			{
				subjBytes[i] = 'A';
			}
			final String subject = new String(subjBytes);

			byte[] replyBytes = new byte[size];
			for (int i = 0; i < size; i++)
			{
				replyBytes[i] = 'A';
			}
			final String reply = new String(replyBytes);

			final Channel<Boolean> ch = new Channel<Boolean>();
			try (AsyncSubscription s = c.subscribeAsync(subject, new MessageHandler() {
				@Override
				public void onMessage(Message msg) {
					assertEquals(subject.length(), msg.getSubject().length());
					assertEquals(subject, msg.getSubject());
					assertEquals(reply.length(), msg.getReplyTo().length());
					assertEquals(reply, msg.getReplyTo());
					ch.add(true);
				}
			})) {

				c.publish(subject, reply, (byte[])null);
				try {
					c.flush();
				} catch (Exception e) {
					e.printStackTrace();
					fail(e.getMessage());
				}

				assertTrue(ch.get(5, TimeUnit.SECONDS));
			}
		} catch (IOException | TimeoutException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	public class CountHandler implements MessageHandler {

		AtomicInteger counter = new AtomicInteger();
		CountHandler(AtomicInteger c) {
			this.counter = c;
		}
		@Override
		public void onMessage(Message msg) {
			counter.incrementAndGet();
		}

	}

	@Test
	public void testAsyncSubHandlerAPI() throws Exception
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			final AtomicInteger received = new AtomicInteger();
			MessageHandler h = new MessageHandler() {
				public void onMessage(Message msg) {
					received.incrementAndGet();
				}
			};

			try (AsyncSubscription s = c.subscribeAsync("foo",  h)) {
				c.publish("foo", null);
				c.flush();
				Thread.sleep(100);
			}

			try (AsyncSubscription s = c.subscribeAsync("foo", "bar", h)) {
				c.publish("foo", null);
				c.flush();
				Thread.sleep(100);
			}
			
			try (AsyncSubscription s = c.subscribeAsync("foo")) {
				s.setMessageHandler(h);
				s.start();
				c.publish("foo", null);
				c.flush();
				Thread.sleep(100);
			}

			assertEquals(3, received.get());
		}
	}

	@Test
	public void testManyRequests() {
		int numMsgs = 500;
		try (NATSServer ts = new NATSServer()) {
			sleep(500);
			ConnectionFactory cf = new ConnectionFactory(ConnectionFactory.DEFAULT_URL);
			try (final Connection conn = cf.createConnection()) {
				try (Subscription s = conn.subscribe("foo", new MessageHandler() {
					public void onMessage(Message message) {
						try {
							conn.publish(message.getReplyTo(), "response".getBytes());
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				})) {
					for (int i = 0; i < numMsgs; i++) {
						try {
//							System.out.println(conn.request("foo", "request".getBytes(), 5000));
							conn.request("foo", "request".getBytes(), 5000);
						} catch (TimeoutException e) {
							System.err.println("got timeout " + i);
							fail("timed out: " + i);
						} catch (IOException e) {
							fail(e.getMessage());
						}
					}
				} 
			} catch (IOException | TimeoutException e1) {
				fail(e1.getMessage());
			}
		}
	}
}
