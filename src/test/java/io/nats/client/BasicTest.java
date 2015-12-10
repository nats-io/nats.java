package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class BasicTest {
	@Rule
	public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

	ExecutorService executor = Executors.newFixedThreadPool(5);
	UnitTestUtilities utils = new UnitTestUtilities();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		utils.startDefaultServer();
	}

	@After
	public void tearDown() throws Exception {
		utils.stopDefaultServer();
	}

	@Test
	public void testConnectedServer() throws IOException, TimeoutException
	{

		Connection c = new ConnectionFactory().createConnection();

		String u = c.getConnectedUrl();

		assertFalse(String.format("Invalid connected url %s.", u),
				u == null || "".equals(u.trim()));

		assertTrue(String.format("Invalid connected url %s.", u),
				Constants.DEFAULT_URL.equals(u));

		c.close();
		u = c.getConnectedUrl();

		assertTrue("Url is not null after connection is closed.", u == null);

	}

	@Test
	public void testMultipleClose() throws IOException, TimeoutException
	{
		final Connection c = new ConnectionFactory().createConnection();

		List<Callable<Object>> todo = new ArrayList<Callable<Object>>(10);

		for (int i = 0; i < 10; i++)
		{
			todo.add(Executors.callable(new Runnable() {
				@Override 
				public void run() {
					c.close();
				}
			}));			
		}
		try {
			List<Future<Object>> answers = executor.invokeAll(todo);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		c.close();
	}

	@Test(expected=IllegalArgumentException.class)
	public void testBadOptionTimeoutConnect()
	{
		ConnectionFactory cf = new ConnectionFactory();
		cf.setConnectionTimeout(-1);
	}

	@Test
	public void testSimplePublish() 
			throws ConnectionClosedException, IOException, TimeoutException
	{
		Connection c = new ConnectionFactory().createConnection();
		c.publish("foo", "Hello World!".getBytes());
		c.close();
	}

	@Test
	public void testSimplePublishNoData()
			throws ConnectionClosedException, IOException, TimeoutException
	{
		Connection c = new ConnectionFactory().createConnection();
		c.publish("foo", null);
		c.close();
	}

	private boolean compare(byte[] p1, byte[] p2)
	{
		return Arrays.equals(p1, p2);
	}

	private boolean compare(byte[] payload, Message m)
	{
		return Arrays.equals(payload, m.getData());
	}

	private boolean compare(Message a, Message b)
	{
		if (a.getSubject().equals(b.getSubject()) == false)
			return false;

		if (a.getReplyTo() != null && a.getReplyTo().equals(b.getReplyTo()))
		{
			return false;
		}

		return compare(a.getData(), b.getData());
	}

	final byte[] omsg = "Hello World".getBytes();
	final Object mu = new Object();
	AsyncSubscription asyncSub = null;
	boolean received = false;

	@Test
	public void testAsyncSubscribe() throws Exception
	{
		Connection c = new ConnectionFactory().createConnection();
		AsyncSubscription s = c.subscribeAsync("foo", new CheckReceivedAndValidHandler());
		asyncSub = s;
		s.start();

		synchronized(mu) 
		{
			received = false;
			c.publish("foo", omsg);
			c.flush();
			mu.wait(30000);
			//				Monitor.Wait(mu, 30000);
		}

		assertTrue("Did not receive message.", received);
		c.close();
	}

	private class CheckReceivedAndValidHandler implements MessageHandler
	{

		@Override
		public void onMessage(Message msg) {
			if (compare(msg.getData(), omsg) == false)
				fail("Messages are not equal.");

			if (msg.getSubscription() != asyncSub)
				fail("Subscriptions do not match.");

			synchronized(mu)
			{
				received = true;
				mu.notify();
			}
		}
	}

	@Test
	public void testSyncSubscribe() throws IOException, TimeoutException
	{
		Connection c = new ConnectionFactory().createConnection();
		SyncSubscription s = c.subscribeSync("foo");
		c.publish("foo", omsg);
		try {
		Message m = s.nextMessage(3000);
		assertTrue("Messages are not equal.", compare(omsg, m));
		} catch (IOException e) {
			throw e;
		} catch (TimeoutException e) {
			throw e;
		} finally {
			c.close();
		}
	}

	@Test
	public void testPubWithReply() throws Exception
	{
		Connection c = new ConnectionFactory().createConnection();
		SyncSubscription s = c.subscribeSync("foo");
		Thread.sleep(500);
		c.publish("foo", "reply", omsg);
		Message m = s.nextMessage(3000);
		assertTrue("Messages are not equal.", compare(omsg, m));
		c.close();
	}

	@Test
	public void testFlush() throws Exception
	{
		Connection c = new ConnectionFactory().createConnection();
		SyncSubscription s = c.subscribeSync("foo");
		c.publish("foo", "reply", omsg);
		c.flush();
		c.close();
	}

	@Test
	public void testQueueSubscriber() throws Exception	
	{
		Connection c = new ConnectionFactory().createConnection();
		SyncSubscription s1 = c.subscribeSync("foo", "bar"),
				s2 = c.subscribeSync("foo", "bar");
		c.publish("foo", omsg);
		c.flush(1000);

		assertTrue("Invalid message count in queue.", 
				s1.getQueuedMessageCount() + s2.getQueuedMessageCount() == 1);

		// Drain the messages.
		try { s1.nextMessage(100); }
		catch (TimeoutException te) { }

		try { s2.nextMessage(100); }
		catch (TimeoutException te) { }

		int expected = 1000;

		for (int i = 0; i < 1000; i++)
		{
			c.publish("foo", omsg);
		}
		c.flush(2000);

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}

		int r1 = s1.getQueuedMessageCount();
		int r2 = s2.getQueuedMessageCount();

		int actual = r1 + r2;
		assertTrue(String.format("Incorrect number of messages: %d vs %d",
				actual, expected), 
				(r1 + r2) == expected);

		assertTrue(String.format("Too much variance between %d and %d", r1, r2), 
				Math.abs(r1 - r2) <= (expected * .15));
		c.close();
	}

	@Test
	public void testReplyArg() throws Exception
	{
		Connection c = new ConnectionFactory().createConnection();
		ExpectedReplyHandler cb = new ExpectedReplyHandler();
		AsyncSubscription s = c.subscribeAsync("foo", cb);
		s.start();

		synchronized(mu)
		{
			received = false;
			c.publish("foo", "bar", null);
			mu.wait(5000);
		}

		assertTrue("Expected \"bar\", received: " + cb.getReply(), 
				"bar".equals(cb.getReply()));

		assertTrue("Message not received.", received==true);
		c.close();
	}

	protected class ExpectedReplyHandler implements MessageHandler
	{
		String reply = null;

		String getReply() {
			return reply;
		}

		@Override
		public void onMessage(Message msg) {

			synchronized(mu)
			{
				reply = msg.getReplyTo();
				received = true;
				mu.notify();
			}
		}
	}

	@Test
	public void testSyncReplyArg() throws Exception
	{
		Connection c = new ConnectionFactory().createConnection();
		SyncSubscription s = c.subscribeSync("foo");
		c.publish("foo", "bar", null);
		c.flush(30000);

		Message m = s.nextMessage(1000);
		assertTrue("Expected \"bar\", received: " + m,
				"bar".equals(m.getReplyTo()));
		c.close();
	}

	class TestUnsubscribeHandler implements MessageHandler
	{
		int count = 0, max = 0;
		boolean unsubscribed=false;
		AsyncSubscription asyncSub = null;
		Exception lastEx = null;

		TestUnsubscribeHandler(AsyncSubscription s, int max) {
			this.asyncSub = s;
			this.max=max;
		}

		Exception getLastEx() {
			return lastEx;
		}
		@Override
		public void onMessage(Message m) {
			count++;
			if (count == max)
			{
				try {
					asyncSub.unsubscribe();
				} catch (Exception e) {
					lastEx = e;
				}
				synchronized(mu)
				{
					unsubscribed = true;
					mu.notify();
				}
			}
		}
		synchronized boolean isUnsubscribed() {
			return unsubscribed;
		}
		int getCount() {
			return count;
		}
	}

	@Test
	public void testUnsubscribe() throws Exception
	{
		int max = 20;
		ConnectionFactory cf = new ConnectionFactory();
		cf.setReconnectAllowed(false);
		final Connection c = cf.createConnection();
		AsyncSubscription s = null;
		s = c.subscribeAsync("foo");
		TestUnsubscribeHandler handler = new TestUnsubscribeHandler(s, max);
		s.setMessageHandler(handler);

		s.start();

		for (int i = 0; i < max; i++)
		{
			c.publish("foo", null, null);
		}
		Thread.sleep(100);
		c.flush();

		synchronized(mu)
		{
			if (!handler.isUnsubscribed())
			{
				try {
					mu.wait(5000);
				} catch (InterruptedException e) {}
			}
		}
		int total = handler.getCount();
		assertTrue(handler.getLastEx()==null);
		//		handler.getLastEx().printStackTrace();
		assertTrue(String.format(
				"Received wrong # of messages after unsubscribe: %d vs %d",
				total, max), total == max);
		//		s.unsubscribe();
		c.close();
	}

	@Test(expected=IllegalStateException.class)
	public void testDoubleUnsubscribe() throws ConnectionClosedException, 
	BadSubscriptionException, IOException, TimeoutException, NATSException
	{
		Connection c = new ConnectionFactory().createConnection();
		SyncSubscription s = c.subscribeSync("foo");
		s.unsubscribe();
		try {
			s.unsubscribe();
		} catch (IllegalStateException e) {
			throw e;
		} finally {
			c.close();
		}
	}

	@Test(expected=TimeoutException.class)
	public void testRequestTimeout() throws TimeoutException, ConnectionClosedException, IOException
	{
		Connection c = new ConnectionFactory().createConnection();
		assertFalse(c.isClosed());
		try {
			c.request("foo", "help".getBytes(), 3000);
		} catch (TimeoutException e) {
			throw e;
		}
		finally {
			c.close();
		}
		System.err.println("Done");
	}

	@Test
	public void testRequest() throws Exception
	{
		final byte[] response = 
				"I will help you.".getBytes();
		final Connection c = new ConnectionFactory().createConnection();
		AsyncSubscription s = c.subscribeAsync("foo",  
				new MessageHandler() {
			@Override 
			public void onMessage(Message m)
			{
				try {
					c.publish(m.getReplyTo(), response);
					c.flush();
				} catch (Exception e){}
			}
		});

		final byte[] request = "help".getBytes();
		Message m = c.request("foo", request, 5000);

		assertTrue("Response isn't valid.", compare(m.getData(), response));
		c.close();
	}

	@Test
	public void testRequestNoBody() throws Exception
	{
		final byte[] response = 
				"I will help you.".getBytes();

		final Connection c = new ConnectionFactory().createConnection();
		AsyncSubscription s = c.subscribeAsync("foo",  
				new MessageHandler() {
			@Override 
			public void onMessage(Message m)
			{
				try {
					c.publish(m.getReplyTo(), response);
				} catch (Exception e){}
			}
		});

		s.start();

		Message m = c.request("foo", null, 50000);

		assertTrue("Response isn't valid.", compare(m.getData(), response));
		c.close();

	}

	private class FlushHandler implements MessageHandler {
		Connection c;
		Exception lastEx = null;
		FlushHandler(Connection c) {
			this.c = c;
		}
		public Exception getLastEx() {
			return lastEx;
		}
		@Override
		public void onMessage(Message msg) {
			try {
				c.flush();
			} catch (Exception e1) {
				lastEx = e1;
			}

			synchronized(mu)
			{
				try {

					mu.notify();
				} catch (Exception e){}
			}
		}

	}

	@Test
	public void testFlushInHandler() throws Exception
	{
		final Connection c = new ConnectionFactory().createConnection();
		FlushHandler flushCb = new FlushHandler(c); 
		AsyncSubscription s = c.subscribeAsync("foo", flushCb);

		s.start();

		synchronized(mu)
		{
			c.publish("foo", "Hello".getBytes(Charset.forName("UTF-8")));
			mu.wait();
		}
		Exception e = flushCb.getLastEx();
		if (e != null)
			throw e;
		c.close();
	}

	@Test
	public void testReleaseFlush() throws Exception
	{
		final Connection c = new ConnectionFactory().createConnection();
		byte[] data = "Hello".getBytes(Charset.forName("UTF-8"));
		for (int i = 0; i < 1000; i++)
		{
			c.publish("foo", data);
		}

		executor.execute(new Runnable() {
			@Override 
			public void run() {
				c.close();
			}
		});
		c.flush();
		c.close();
	}

	@Test
	public void testCloseAndDispose() throws IOException, TimeoutException
	{
		Connection c = new ConnectionFactory().createConnection();
		c.close();
	}

	@Test
	public void testInbox() throws IOException, TimeoutException
	{
		Connection c = new ConnectionFactory().createConnection();
		String inbox = c.newInbox();
		assertFalse("inbox was null or whitespace", 
				inbox.equals(null) || inbox.trim().length()==0);
		assertTrue(inbox.startsWith("_INBOX."));
		c.close();
	}

	@Test
	public void testStats() throws Exception
	{
		Connection c = new ConnectionFactory().createConnection();
		byte[] data = 
				"The quick brown fox jumped over the lazy dog".getBytes(
						Charset.forName("UTF-8"));
		int iter = 10;

		for (int i = 0; i < iter; i++)
		{
			c.publish("foo", data);
		}
		c.flush(1000);

		Statistics stats = c.getStats();
		assertEquals(iter, stats.getOutMsgs());
		assertEquals(iter * data.length, stats.getOutBytes());

		c.resetStats();

		// Test both sync and async versions of subscribe.
		AsyncSubscription s1 = c.subscribeAsync("foo", new MessageHandler() {

			@Override
			public void onMessage(Message msg) {

			}

		});

		//			s1.MessageHandler += (sender, arg) => { };
		s1.start();

		SyncSubscription s2 = c.subscribeSync("foo");

		for (int i = 0; i < iter; i++)
		{
			c.publish("foo", data);
		}
		c.flush(1000);

		stats = c.getStats();
		assertEquals(2 * iter, stats.getInMsgs());
		assertEquals(2 * iter * data.length, stats.getInBytes());
		c.close();
	}

	@Test
	public void testRaceSafeStats() throws IOException, TimeoutException
	{
		final Connection c = new ConnectionFactory().createConnection();

		//			new Task(() => { c.publish("foo", null); }).Start();
		executor.execute(new Runnable() {

			@Override
			public void run() {
				try {
					c.publish("foo", null);
				} catch (ConnectionClosedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}

		});
		try {Thread.sleep(1000);} catch (InterruptedException e) {}

		assertEquals(1, c.getStats().getOutMsgs());
		c.close();
	}

	@Test
	public void testBadSubject() throws IOException, TimeoutException
	{
		final Connection c = new ConnectionFactory().createConnection();
		//			new Task(() => { c.publish("foo", null); }).Start();
		executor.execute(new Runnable() {

			@Override
			public void run() {
				try {
					c.publish("foo", null);
				} catch (ConnectionClosedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}

		});
		try { Thread.sleep(200);} catch (InterruptedException e) {}

		assertEquals(1, c.getStats().getOutMsgs());
		c.close();
	}

	public class CaptureHandler implements MessageHandler {

		Message message = null;
		final Object testLock;

		CaptureHandler(Object obj) {
			testLock = obj;
		}

		@Override
		public void onMessage(Message msg) {
			this.message=msg;
			synchronized(testLock)
			{
				testLock.notify();
			}
		}

		public Message getMsg() {
			return message;
		}
	}

	@Test
	public void testLargeMessage() throws Exception
	{
		final Connection c = new ConnectionFactory().createConnection();
		int msgSize = 51200;
		final byte[] msg = new byte[msgSize];
		byte[] output = null;
		for (int i = 0; i < msgSize; i++)
			msg[i] = (byte)'A';

		msg[msgSize-1] = (byte)'Z';

		final Object testLock = new Object();
		CaptureHandler handler = new CaptureHandler(testLock);
		AsyncSubscription s = c.subscribeAsync("foo", handler);
		s.start();

		c.publish("foo", msg);
		c.flush(1000);

		synchronized(testLock)
		{
			try {
				testLock.wait(2000);
			} catch (InterruptedException e) {}
			output = handler.getMsg().getData();
		}
		assertTrue("Response isn't valid.", compare(msg, output));
		c.close();
	}

	@Test
	public void testSendAndRecv() throws Exception
	{
		Connection c = new ConnectionFactory().createConnection();
		final AtomicInteger received = new AtomicInteger();
		int count = 1000;
		AsyncSubscription s = c.subscribeAsync("foo",
				new MessageHandler() {
			@Override
			public void onMessage(Message msg) {
				received.incrementAndGet();
			}
		});

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
		c.close();
	}

	@Test
	public void testLargeSubjectAndReply() throws Exception
	{
		Connection c = new ConnectionFactory().createConnection();
		String subject = "";
		for (int i = 0; i < 1024; i++)
		{
			subject += "A";
		}

		String reply = "";
		for (int i = 0; i < 1024; i++)
		{
			reply += "A";
		}

		Object testLock = new Object();
		CaptureHandler cb = new CaptureHandler(testLock);
		AsyncSubscription s = c.subscribeAsync(subject, cb);

		s.start();

		c.publish(subject, reply, null);
		c.flush();

		synchronized(testLock)
		{
			testLock.wait(1000);
		}
		Message m = cb.getMsg();
		assertTrue("Invalid subject received.", subject.equals(m.getSubject()));
		assertTrue("Invalid subject received.", reply.equals(m.getReplyTo()));
		c.close();
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
		Connection c = new ConnectionFactory().createConnection();
		AtomicInteger received = new AtomicInteger();
		CountHandler h = new CountHandler(received);

		AsyncSubscription s = c.subscribeAsync("foo", h);
		c.publish("foo", null);
		c.flush();
		Thread.sleep(500);
		s.unsubscribe();

		s = c.subscribeAsync("foo", "bar", h);
		c.publish("foo", null);
		c.flush();
		Thread.sleep(500);

		int actual = received.get();
		assertTrue(String.format("Received (%d) != 2", actual), 
				actual==2);
		c.close();
	}
}
