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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import static io.nats.client.UnitTestUtilities.*;

@Category(BenchmarkTest.class)
public class BenchTest {
	ExecutorService executor = Executors.newCachedThreadPool(new NATSThreadFactory("natsbench"));

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
	public void testPubSpeed() {
		int count = 120000000;
		String url = ConnectionFactory.DEFAULT_URL;
		String subject = "foo";
		byte[] payload = null;
		long elapsed = 0L;

		try (Connection c = new ConnectionFactory(url).createConnection()) {

			long t0 = System.nanoTime();

			for (int i = 0; i < count; i++)
			{
				c.publish(subject, payload);
			}

			assertEquals(count, c.getStats().getOutMsgs());

			// Make sure they are all processed
			try { c.flush(); } catch (Exception e) {}

			long t1 = System.nanoTime();

			elapsed = TimeUnit.NANOSECONDS.toSeconds(t1 - t0);
			System.out.println("Elapsed time is " + elapsed + " seconds");

			System.out.printf("Published %d msgs in %d seconds ", count, elapsed);
			if (elapsed > 0) {
				System.out.printf("(%d msgs/second).\n",
						(int)(count / elapsed));
			} else {
				fail("Test not long enough to produce meaningful stats.");
			} 
			printStats(c);
		} catch (IOException | TimeoutException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testPubSubSpeed() {
		final int count = 10000000;
		final int MAX_BACKLOG = 32000;
		//		final int count = 2000;
		final String url = ConnectionFactory.DEFAULT_URL;
		final String subject = "foo";
		final byte[] payload = "test".getBytes();
		long elapsed = 0L;

		final Channel<Boolean> ch = new Channel<Boolean>();
		final Channel<Boolean> ready = new Channel<Boolean>();
		final Channel<Long> slow = new Channel<Long>();

		final AtomicInteger received = new AtomicInteger(0);

		final ConnectionFactory cf = new ConnectionFactory(url);

		cf.setExceptionHandler(new ExceptionHandler() {
			public void onException(NATSException e) {
				System.err.println("Error: " + e.getMessage());
				Subscription sub = e.getSubscription();
				System.err.printf("Queued = %d\n", sub.getQueuedMessageCount());
				e.printStackTrace();
//				fail(e.getMessage());
			}
		});

		Runnable r = new Runnable() {
			public void run() {
				try (Connection nc1 = cf.createConnection()) {
					Subscription sub = nc1.subscribe(subject, new MessageHandler() {
						public void onMessage(Message msg) {
							int numReceived = received.incrementAndGet();
							if (numReceived>=count) {
								ch.add(true);
							}
						}
					});
					ready.add(true);
					while (received.get() < count) {
						if (sub.getQueuedMessageCount() > 8192) {
							System.err.printf("queued=%d\n", sub.getQueuedMessageCount());
							if (slow.getCount()==0) {
								slow.add((long)sub.getQueuedMessageCount());
							}
							sleep(1);
						}
					}
					System.out.println("nc1 (subscriber):\n=======");
					printStats(nc1);
				} catch (IOException | TimeoutException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};

		Thread subscriber = new Thread(r);
		subscriber.start();

		try (final Connection nc2 = cf.createConnection()) {
			long t0 = System.nanoTime();
			long numSleeps=0L;

			ready.get();
			int sleepTime = 100;

			for (int i=0; i<count; i++)
			{
				nc2.publish(subject, payload);
				// Don't overrun ourselves and be a slow consumer, server will cut us off
				if ((i-received.get()) > MAX_BACKLOG) {
					int rec = received.get();
					int sent = i+1;
					int backlog = sent - rec;
					
					System.err.printf("sent=%d, received=%d, backlog=%d, ratio=%.2f, sleepInt=%d\n",
							sent, rec, backlog, (double)backlog/cf.getMaxPendingMsgs(), sleepTime);

//					sleepTime += 100;
					sleep(sleepTime);
					numSleeps++;
				}
			}
			System.err.println("numSleeps="+numSleeps);

			// Make sure they are all processed
			try {
				String s = String.format("Timed out waiting for messages, received %d/%d", 
						received.get(), count);
				assertTrue(s, ch.get(30,TimeUnit.SECONDS));
			} catch (TimeoutException e) {
				fail(String.format("Timed out waiting for delivery completion, received %d/%d", 
						received.get(), count));
			}
			assertEquals(count, received.get());
			try {
				subscriber.join();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			long t1 = System.nanoTime();

			elapsed = TimeUnit.NANOSECONDS.toSeconds(t1 - t0);
			System.out.println("Elapsed time is " + elapsed + " seconds");

			System.out.printf("Pub/Sub %d msgs in %d seconds ", count, elapsed);
			if (elapsed > 0) {
				System.out.printf("(%d msgs/second).\n",
						(int)(count / elapsed));
			} else {
				fail("Test not long enough to produce meaningful stats.");
			} 
			System.out.println("nc2 (publisher):\n=======");
			printStats(nc2);

		} catch (IOException | TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private void printStats(Connection c)
	{
		Statistics s = c.getStats();
		System.out.println("Statistics:  ");
		System.out.printf("   Outgoing Payload Bytes: %d\n", s.getOutBytes());
		System.out.printf("   Outgoing Messages: %d\n", s.getOutMsgs());
		System.out.printf("   Flushes: %d\n", s.getFlushes());
	}

	public static void main(String[] args) {
		BenchTest b = new BenchTest();
		b.testPubSubSpeed();
	}
}
