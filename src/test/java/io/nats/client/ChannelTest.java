/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class ChannelTest {
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
	public void testChannelLinkedBlockingQueue() {
		@SuppressWarnings("unchecked")
		LinkedBlockingQueue<Message> queue = 
				(LinkedBlockingQueue<Message>) 
				mock(LinkedBlockingQueue.class);
		Channel<Message> ch = new Channel<Message>(queue);
		assertNotNull(ch);
	}
	@Test
	public void testChannelCollection() {
		Collection<Message> msgList = new ArrayList<Message>();
		Channel<Message> ch = new Channel<Message>(msgList);
		assertNotNull(ch);
	}
}
