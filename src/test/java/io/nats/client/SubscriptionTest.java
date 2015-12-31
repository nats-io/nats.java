package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionTest {
	final Logger logger = LoggerFactory.getLogger(SubscriptionTest.class);

	ExecutorService executor = Executors.newCachedThreadPool(new NATSThreadFactory("nats-test-thread"));

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
		UnitTestUtilities.startDefaultServer();
	}

	@After
	public void tearDown() throws Exception {
		UnitTestUtilities.stopDefaultServer();
	}

	@Test
	public void testServerAutoUnsub() throws Exception
	{
		try (Connection c = new ConnectionFactory().createConnection())
		{
			assertFalse(c.isClosed());
			final AtomicLong received = new AtomicLong(0L);
			int max = 10;

			try (AsyncSubscription s = c.subscribeAsync("foo", new MessageHandler () {

				@Override
				public void onMessage(Message msg) {
					System.out.println("Received msg.");
					received.incrementAndGet();
				}

			}))
			{
				s.autoUnsubscribe(max);
				s.start();
				assertTrue(s.isValid());

				for (int i = 0; i < (max * 2); i++)
				{
					c.publish("foo", "hello".getBytes("UTF-8"));
				}
				c.flush();

				try {Thread.sleep(100); } catch (InterruptedException e) {}

				assertTrue(String.format("Received (%d) != max (%d)",
						received.get(), max),
						received.get()==max);

				assertFalse(s.isValid());
			}
		}
	}

	@Test
	public void testClientAutoUnsub() throws IllegalStateException, Exception
	{
		try (Connection c = new ConnectionFactory().createConnection()) {
			assertFalse(c.isClosed());

			long received = 0;
			int max = 10;

			try (SyncSubscription s = c.subscribeSync("foo")) {
				s.autoUnsubscribe(max);

				for (int i = 0; i < max * 2; i++)
				{
					c.publish("foo", null);
				}
				c.flush();

				try { Thread.sleep(100);
				} catch (InterruptedException e1) {
				}

				try
				{
					while (true)
					{
						s.nextMessage(0);
						received++;
					}
				}
				catch (BadSubscriptionException e) { /* ignore */ }

				assertTrue(received == max);
				assertFalse(s.isValid());
			}
		}
	}


	@Test
	public void testCloseSubRelease() throws IOException, TimeoutException
	{
		try (final Connection c = new ConnectionFactory().createConnection())
		{
			try (SyncSubscription s = c.subscribeSync("foo"))
			{
				long t0 = System.nanoTime();
				try
				{
					executor.execute(new Runnable() {
						@Override
						public void run() {
							try { Thread.sleep(10); } catch (InterruptedException e) {}
							c.close();
							assertTrue(c.isClosed());
						}
					});
					s.nextMessage(10000);
				}
				catch (Exception e) { /* ignore */ }

				long elapsed = System.nanoTime()-t0;
				assertTrue("wait time was: " + TimeUnit.NANOSECONDS.toMillis(elapsed),
						TimeUnit.NANOSECONDS.toMillis(elapsed) <= 20);
			} catch (Exception e1) {
				e1.printStackTrace();
				fail(e1.getMessage());
			}	
		}
	}

	@Test
	public void testValidSubscriber() 
			throws TimeoutException, IllegalStateException, IOException
	{
		try (final Connection c = new ConnectionFactory().createConnection())
		{
			try (SyncSubscription s = c.subscribeSync("foo")) {
				assertTrue(s.isValid());

				try { s.nextMessage(100); }
				catch (TimeoutException e) { }

				assertTrue(s.isValid());

				s.unsubscribe();

				assertFalse(s.isValid());

				try { s.nextMessage(100); }
				catch (BadSubscriptionException e) { }
			} catch (Exception e1) {
				e1.printStackTrace();
				fail(e1.getMessage());
			}
		}
	}

	@Test
	public void testSlowSubscriber() throws Exception
	{
		ConnectionFactory cf = new ConnectionFactory();
		cf.setSubChanLen(100);

		try (final Connection c = cf.createConnection())
		{
			try (SyncSubscription s = c.subscribeSync("foo")) {
				for (int i=0; i < (cf.getSubChanLen()+100); i++)
				{
					c.publish("foo", "Hello".getBytes());
				}

				try
				{
					int timeout = 5000;
					long t0 = System.nanoTime();
					c.flush(timeout);
					long elapsed = TimeUnit.NANOSECONDS.toMillis(
							System.nanoTime()-t0);
					assertFalse(
							String.format(
									"Flush did not return before timeout: %d >= %d",
									elapsed, timeout),
							elapsed >= timeout);
				}
				catch (SlowConsumerException e) {
					// NOOP: This is expected
				}
				catch (Exception ex)
				{
					logger.error("Exception: ", ex);
					if (ex.getCause() != null)
						logger.error("Caused by: ", ex.getCause());
					throw ex;
				}

				try 
				{
					s.nextMessage(500);
				}
				catch (SlowConsumerException e)
				{
					return;
				}
			}
		}

		fail("Did not receive an exception.");
	}

	@Test
	public void testSlowAsyncSubscriber() throws IOException, TimeoutException
	{
		ConnectionFactory cf = new ConnectionFactory();
		cf.setSubChanLen(10);

		try (final Connection c = cf.createConnection()) {
			final Object mu = new Object();
			try (final AsyncSubscription s = c.subscribeAsync("foo", new MessageHandler() {
				@Override
				public void onMessage(Message msg) {
					synchronized(mu)
					{
						System.out.println("Subscriber Waiting....");
						long t0 = System.nanoTime();
						try {mu.wait(30000);} catch (InterruptedException e) {}
						long elapsed = TimeUnit.NANOSECONDS.toMillis(
								System.nanoTime() - t0);
						assertTrue(elapsed < 20000);
						System.out.println("Subscriber done.");
					}
				}
			})) {

				System.err.println("subChanLen = " + cf.getSubChanLen());
				for (int i = 0; i < (cf.getSubChanLen() + 100); i++)
				{
					c.publish("foo", "Hello".getBytes());
				}

				int flushTimeout = 5000;

				long t0 = System.nanoTime();
				long elapsed = 0L;
				boolean exThrown = false;
				try
				{
					System.out.println("flushing: " + flushTimeout + " msec" );
					c.flush(flushTimeout);
					System.out.println("flush complete." );
				}
				catch (Exception e)
				{
					exThrown = true;
				}
				elapsed = TimeUnit.NANOSECONDS.toMillis(
						System.nanoTime() - t0);
				assertTrue("Flush did not return before timeout, elabsed msec="+elapsed, 
						elapsed < flushTimeout);

				synchronized(mu)
				{
					System.out.println("notifying messageCB");
					mu.notify();
				}

				assertTrue("Expected an error indicating slow consumer", exThrown);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				fail(e1.getMessage());
			}
		}
	}

	@Test
	public void testAsyncErrHandler() throws IllegalStateException, IOException, TimeoutException, Exception
	{
		final Object subLock = new Object();
		final Object testLock = new Object();
		final AtomicBoolean handledError = new AtomicBoolean(false);
		final AtomicBoolean blockedOnSubscriber = new AtomicBoolean(false);

		ConnectionFactory cf = new ConnectionFactory();
		cf.setSubChanLen(10);

		Connection c = cf.createConnection();

		final AsyncSubscription s = c.subscribeAsync("foo", new MessageHandler()
		{
			@Override
			public void onMessage(Message msg) {
				synchronized(subLock)
				{
					if (blockedOnSubscriber.get())
						return;

					System.out.println("Subscriber Waiting....");
					long t0 = System.nanoTime();
					try { subLock.wait(10000);} catch (InterruptedException e) {
					}
					long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
					assertTrue("monitor timed out", elapsed < 10000 );
					System.out.println("Subscriber done.");
					blockedOnSubscriber.set(true);
				}
			}
		});
		c.setExceptionHandler(new ExceptionHandler()
		{
			public void onException(Connection conn, Subscription sub, 
					Throwable e) {
				synchronized(subLock)
				{
					if (handledError.get())
						return;

					handledError.set(true);

					assertTrue(sub == s);

					assertTrue("Expected SlowConsumerException, but got " + e, 
							e instanceof SlowConsumerException);

					// release the subscriber
					subLock.notify();
				}
				// release the test
				synchronized(testLock) { testLock.notify(); }
			}
		});


		synchronized(testLock)
		{

			for (int i = 0; i < (cf.getSubChanLen() + 100); i++)
			{
				c.publish("foo", null);
			}
			try {
				c.flush(1000);
			} catch (SlowConsumerException e)
			{
				// NOOP - expected here
			}

			long t0 = System.nanoTime();
			testLock.wait(10000);
			long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
			assertTrue("monitor timed out", elapsed < 10000 );
		}
		c.close();
	}

	@Test
	public void testAsyncSubscriberStarvation() throws Exception
	{
		//		final Object waitCond = new Object();
		final Lock cbLock = new ReentrantLock();
		final Condition cbFired = cbLock.newCondition();
		try (final Connection c = new ConnectionFactory().createConnection())
		{
			AsyncSubscription helper = c.subscribeAsync("helper",
					new MessageHandler()
			{
				@Override
				public void onMessage(Message msg) {
					System.err.println("Helper");
					c.publish(msg.getReplyTo(),
							"Hello".getBytes(Charset.forName("UTF-8")));				
				}
			});
			
			try {Thread.sleep(100);} catch (InterruptedException e) {}

			AsyncSubscription start = c.subscribeAsync("start", new MessageHandler()
			{
				@Override
				public void onMessage(Message msg) {
					System.err.println("Responder");
					String responseIB = c.newInbox();
					AsyncSubscription ia = c.subscribe(responseIB,
							new MessageHandler()
					{
						@Override
						public void onMessage(Message msg) {
							System.err.println("Internal subscriber.");
							cbLock.lock();
							try {
								cbFired.signal(); 
							} finally {
								cbLock.unlock();
							}
						}
					});
					c.publish("helper", responseIB,
							"Help me!".getBytes(Charset.forName("UTF-8")));
				}
			});

			try {Thread.sleep(100);} catch (InterruptedException e) {}

//			c.publish("start", "Begin".getBytes(Charset.forName("UTF-8")));
//			System.err.println("Started");
//			c.flush();

			cbLock.lock();
			try { 
				c.publish("start", "Begin".getBytes(Charset.forName("UTF-8")));
				System.err.println("Started");
				c.flush();
				assertTrue("Was stalled inside of callback waiting on another callback",
						cbFired.await(2, TimeUnit.SECONDS));
			} finally {
				cbLock.unlock();
			}
		}
	}

	@Test
	public void TestAsyncSubscribersOnClose() throws IllegalStateException, IOException, TimeoutException, Exception
	{
		/// basically tests if the subscriber sub channel gets
		/// cleared on a close.
		final Object waitCond = new Object();
		final AtomicInteger callbacks = new AtomicInteger(0);

		try (Connection c = new ConnectionFactory().createConnection()) {
			try (AsyncSubscription s = c.subscribe("foo",
					new MessageHandler()
			{
				@Override
				public void onMessage(Message msg) {
					callbacks.getAndIncrement();
					synchronized(waitCond)
					{
						try { waitCond.wait();} 
						catch (InterruptedException e) {}
					}							
				}
			})) 
			{

				for (int i = 0; i < 10; i++)
				{
					c.publish("foo", null);
				}
				c.flush();

				try {Thread.sleep(500);
				} catch (InterruptedException e) {}
				c.close();

				synchronized(waitCond)
				{
					waitCond.notify();
				}

				try {Thread.sleep(500);
				} catch (InterruptedException e) {}

				assertTrue(callbacks.get() == 1);
			} // AsyncSubscription
		} // Connection
	}

}
