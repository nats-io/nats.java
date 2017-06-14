/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

@Category(UnitTest.class)
public class StatisticsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void tearDown() throws Exception {
    }

    private Statistics createDummyStats() {
        Statistics stats = new Statistics();
        stats.incrementFlushes();
        stats.incrementInBytes(44L);
        stats.incrementInMsgs();
        stats.incrementOutBytes(199L);
        stats.incrementOutMsgs();
        return stats;
    }

    @Test
    public void testClear() {
        Statistics stats = createDummyStats();
        stats.clear();
        assertEquals(0, stats.getFlushes());
        assertEquals(0, stats.getInBytes());
        assertEquals(0, stats.getInMsgs());
        assertEquals(0, stats.getOutBytes());
        assertEquals(0, stats.getOutMsgs());
        assertEquals(0, stats.getReconnects());
    }

    @Test
    public void testCopyConstructor() {
        Statistics s1 = new Statistics();

        s1.incrementInMsgs();
        s1.incrementInBytes(8192);
        s1.incrementOutMsgs();
        s1.incrementOutBytes(512);
        s1.incrementReconnects();

        Statistics s2 = new Statistics(s1);

        assertEquals(s1.getInMsgs(), s2.getInMsgs());
        assertEquals(s1.getOutMsgs(), s2.getOutMsgs());
        assertEquals(s1.getInBytes(), s2.getInBytes());
        assertEquals(s1.getOutBytes(), s2.getOutBytes());
        assertEquals(s1.getReconnects(), s2.getReconnects());

        assertTrue(s2.equals(s2));
    }

    @Test
    public void testToString() {
        assertNotNull(createDummyStats().toString());
    }

    @Test
    public void testGetInMsgs() {
        Statistics stats = createDummyStats();
        stats.getInMsgs();
    }

    @Test
    public void testIncrementInMsgs() {
        Statistics stats = createDummyStats();
        long n1 = stats.getInMsgs();
        stats.incrementInMsgs();
        assertEquals(n1 + 1, stats.getInMsgs());
    }

    @Test
    public void testGetOutMsgs() {
        Statistics stats = createDummyStats();
        stats.getOutMsgs();
    }

    @Test
    public void testIncrementOutMsgs() {
        Statistics stats = createDummyStats();
        long n1 = stats.getOutMsgs();
        stats.incrementOutMsgs();
        assertEquals(n1 + 1, stats.getOutMsgs());
    }

    @Test
    public void testGetInBytes() {
        Statistics stats = createDummyStats();
        stats.getInBytes();
    }

    @Test
    public void testIncrementInBytes() {
        Statistics stats = createDummyStats();
        long n1 = stats.getInBytes();
        stats.incrementInBytes(1);
        assertEquals(n1 + 1, stats.getInBytes());
    }

    @Test
    public void testGetOutBytes() {
        Statistics stats = createDummyStats();
        stats.getOutBytes();
    }

    @Test
    public void testIncrementOutBytes() {
        Statistics stats = createDummyStats();
        long n1 = stats.getOutBytes();
        stats.incrementOutBytes(1);
        assertEquals(n1 + 1, stats.getOutBytes());
    }

    @Test
    public void testGetReconnects() {
        Statistics stats = createDummyStats();
        stats.getReconnects();
    }

    @Test
    public void testIncrementReconnects() {
        Statistics stats = createDummyStats();
        long n1 = stats.getReconnects();
        stats.incrementReconnects();
        assertEquals(n1 + 1, stats.getReconnects());
    }

    @Test
    public void testGetFlushes() {
        Statistics stats = createDummyStats();
        stats.getFlushes();
    }

    @Test
    public void testIncrementFlushes() {
        Statistics stats = createDummyStats();
        long n1 = stats.getFlushes();
        stats.incrementFlushes();
        assertEquals(n1 + 1, stats.getFlushes());
    }
}
