/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

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
public class SyncSubscriptionImplTest {
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
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSyncSubscriptionImplConnectionImplStringStringInt() {
		String subj = "foo", queue = "bar";
		int max = 20;
		
		ConnectionImpl nc = mock(ConnectionImpl.class);
		try (SyncSubscriptionImpl s = new SyncSubscriptionImpl(nc, subj, queue, max))
		{
			s.autoUnsubscribe(max);
			assertEquals(nc, s.getConnection());
			assertEquals(subj, s.getSubject());
			assertEquals(queue, s.getQueue());
			assertEquals(max, s.getMaxPending());
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testProcessMsg() {
		String subj = "foo", queue = "bar";
		int max = 20;
		
		ConnectionImpl nc = mock(ConnectionImpl.class);
		Message m = mock(Message.class);
		try (SyncSubscriptionImpl s = new SyncSubscriptionImpl(nc, subj, queue, max))
		{
			assertTrue(s.processMsg(m));
		}
	}

//	@Test
//	public void testNextMessage() {
//		String subj = "foo", queue = "bar";
//		int max = 20;
//		int timeout = 100;
//
//		ConnectionImpl nc = mock(ConnectionImpl.class);
//		try (SyncSubscriptionImpl s = new SyncSubscriptionImpl(nc, subj, queue, max))
//		{
//			try {
//				Message m = s.nextMessage();
//			} catch (IOException e) {
//				fail(e.getMessage());
//			} 
//		}
//	}
//	
	@Test(expected=TimeoutException.class)
	public void testNextMessageTimeout() throws TimeoutException {
		String subj = "foo", queue = "bar";
		int max = 20;
		int timeout = 100;

		ConnectionImpl nc = mock(ConnectionImpl.class);
		try (SyncSubscriptionImpl s = new SyncSubscriptionImpl(nc, subj, queue, max))
		{
			try {
				Message m = s.nextMessage(timeout);
			} catch (IOException e) {
				fail(e.getMessage());
			} 
		}
	}
}
