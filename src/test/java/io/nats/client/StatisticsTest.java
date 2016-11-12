/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.UnitTestUtilities.setLogLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import ch.qos.logback.classic.Level;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(UnitTest.class)
public class StatisticsTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(StatisticsTest.class);

    private static final LogVerifier verifier = new LogVerifier();

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
        verifier.setup();
    }

    @After
    public void tearDown() throws Exception {
        verifier.teardown();
        setLogLevel(Level.INFO);
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
    public void testClone() throws CloneNotSupportedException {
        Statistics stats = createDummyStats();
        Statistics stats2 = (Statistics) stats.clone();
        assertEquals(stats.getFlushes(), stats2.getFlushes());
        assertEquals(stats.getFlushes(), stats2.getFlushes());
        assertEquals(stats.getInMsgs(), stats2.getInMsgs());
        assertEquals(stats.getOutBytes(), stats2.getOutBytes());
        assertEquals(stats.getOutMsgs(), stats2.getOutMsgs());
        assertEquals(stats.getReconnects(), stats2.getReconnects());
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
