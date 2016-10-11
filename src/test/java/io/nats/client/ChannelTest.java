/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Category(UnitTest.class)
public class ChannelTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(ChannelTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testChannel() {
        new Channel<Message>();
        new Channel<Message>(20);
    }

    @Test
    public void testChannelLinkedBlockingQueue() {
        @SuppressWarnings("unchecked")
        LinkedBlockingQueue<Message> queue =
                (LinkedBlockingQueue<Message>) mock(LinkedBlockingQueue.class);
        Channel<Message> ch = new Channel<Message>(queue);
        assertNotNull(ch);
    }

    @Test
    public void testChannelCollection() {
        Collection<Message> msgList = new ArrayList<Message>();
        Channel<Message> ch = new Channel<Message>(msgList);
        assertNotNull(ch);
    }

    @Test
    public void testGet() throws InterruptedException {
        long num = 22L;
        Channel<Long> ch = new Channel<Long>();
        ch.add(num);
        assertEquals(1, ch.getCount());
        assertEquals(Long.valueOf(num), ch.get());
    }

    @Test
    public void testGetTimeout() throws TimeoutException {
        long num = 22L;
        Channel<Long> ch = new Channel<Long>();
        ch.add(num);
        UnitTestUtilities.sleep(50);
        assertEquals(1, ch.getCount());
        assertEquals(Long.valueOf(num), ch.get(5000));
    }

    @Test
    public void testGetTimeoutUnit() throws TimeoutException {
        long num = 22L;
        Channel<Long> ch = new Channel<Long>();
        ch.add(num);
        assertEquals(1, ch.getCount());
        assertEquals(Long.valueOf(num), ch.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testPoll() throws InterruptedException {
        long num = 22L;
        Channel<Long> ch = new Channel<Long>();
        ch.add(num, 5, TimeUnit.SECONDS);
        assertEquals(Long.valueOf(num), ch.poll());
        assertEquals(0, ch.getCount());
        assertNull(ch.poll());
    }

    @Test
    public void testAdd() {
        long num = 22L;
        Channel<Long> ch = new Channel<Long>();
        ch.add(num);
        assertEquals(1, ch.getCount());
        assertEquals(Long.valueOf(num), ch.get());
    }

    @Test
    public void testAddWhileClosed() throws InterruptedException {
        long num = 22L;
        Channel<Long> ch = new Channel<Long>();
        ch.close();
        assertTrue(ch.isClosed());

        assertFalse(ch.add(num));
        assertEquals(0, ch.getCount());
    }

    @Test
    public void testAddTimeoutUnit() throws InterruptedException {
        long num = 22L;
        Channel<Long> ch = new Channel<Long>();
        ch.add(num, 5, TimeUnit.SECONDS);
        assertEquals(1, ch.getCount());
        assertEquals(Long.valueOf(num), ch.get());
    }

    @Test
    public void testAddTimeoutUnitWhileClosed() throws InterruptedException {
        long num = 22L;
        Channel<Long> ch = new Channel<Long>();
        ch.close();
        assertTrue(ch.isClosed());

        assertFalse(ch.add(num, 5, TimeUnit.SECONDS));
        assertEquals(0, ch.getCount());
    }

    @Test
    public void testClose() throws InterruptedException {
        Channel<Long> ch = new Channel<Long>();
        ch.close();
        assertTrue(ch.isClosed());
    }

    @Test
    public void testIsClosed() {
        Channel<Long> ch = new Channel<Long>();
        assertFalse(ch.isClosed());
        ch.close();
        assertTrue(ch.isClosed());
    }

    @Test
    public void testGetCount() {
        long num = 22L;
        Channel<Long> ch = new Channel<Long>();
        ch.add(num);
        assertEquals(1, ch.getCount());
    }
}
