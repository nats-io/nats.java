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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
		int count = 50000000;
		String url = Constants.DEFAULT_URL;
		String subject = "foo";
		byte[] payload = null;
		long elapsed = 0L;

		try (Connection c = new ConnectionFactory(url).createConnection()) {

			long t0 = System.nanoTime();

			for (int i = 0; i < count; i++)
			{
				c.publish(subject, payload);
			}
			try {
				c.flush();
			} catch (Exception e) {
			}

			long t1 = System.nanoTime();

			elapsed = TimeUnit.NANOSECONDS.toSeconds(t1 - t0);
			System.out.println("Elapsed time is " + elapsed + " seconds");

			System.out.printf("Published %d msgs in %d seconds ", count, elapsed);
			if (elapsed > 0) {
				System.out.printf("(%d msgs/second).\n",
						(int)(count / elapsed));
//				assertTrue(count/elapsed >= 2000000);
//				assertTrue(c.getStats().getFlushes() < 1000);
			} else {
				fail("Test not long enough to produce meaningful stats.");
			} 
			printStats(c);
		} catch (IOException | TimeoutException e) {
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


}
