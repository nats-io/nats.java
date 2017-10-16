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

@Category(IntegrationTest.class)
public class ITBaseTest {
    static int msgDeliveryPoolSize = 0;

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String prop = System.getProperty("msgDeliveryPoolSize");
        if (prop != null) {
            msgDeliveryPoolSize = Integer.parseInt(prop);
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        // If need be, setup the message delivery thread pool for each
        // test. This allows finding out if a test is holding onto
        // a resource in the message callback preventing the pool to
        // be shutdown.
        if (msgDeliveryPoolSize > 0) {
            Nats.createMsgDeliveryThreadPool(msgDeliveryPoolSize);
        }
    }

    @After
    public void tearDown() throws Exception {
        // Shutdown the message delivery pool if one was set.
        Nats.shutdownMsgDeliveryThreadPool();
    }
}