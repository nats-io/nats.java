/*
 *  Copyright (c) 2017 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class BaseUnitTest implements UnitTest {
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // For Unit tests, ensure that there is no global message delivery
        // pool set, which could happen if user is running test suite and
        // environment variable JNATS_MSG_DELIVERY_THREAD_POOL_SIZE happens
        // to be set.
        Nats.shutdownMsgDeliveryThreadPool();
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
}