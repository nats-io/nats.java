/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static io.nats.client.Constants.*;

@Category(UnitTest.class)
public class NATSExceptionTest {
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
	public void testNATSEx() {
		NATSException e = new NATSException();
		assertNotNull(e);
		String msg = "detail message";
		Exception causeEx = new Exception("some other problem");
		Subscription sub = mock(SyncSubscription.class);
		Connection nc = mock(Connection.class);
		
		e = new NATSException(msg);
		assertNotNull(e);
		assertEquals(msg, e.getMessage());
		
		e = new NATSException(causeEx);
		assertNotNull(e);
		assertEquals(causeEx, e.getCause());
		assertEquals("some other problem", e.getCause().getMessage());

		e = new NATSException(causeEx, nc, sub);
		assertNotNull(e);
		assertEquals(causeEx, e.getCause());
		assertEquals("some other problem", e.getCause().getMessage());
		assertEquals(nc, e.getConnection());
		assertEquals(sub, e.getSubscription());
		Connection nc2 = mock(Connection.class);
		e.setConnection(nc2);
		assertEquals(nc2, e.getConnection());
		Subscription sub2 = mock(AsyncSubscription.class);
		e.setSubscription(sub2);
		assertEquals(sub2, e.getSubscription());

		e = new NATSException(msg, causeEx);
		assertNotNull(e);
		assertEquals(msg, e.getMessage());
		assertEquals(causeEx, e.getCause());
		assertEquals("some other problem", e.getCause().getMessage());
	}

}
