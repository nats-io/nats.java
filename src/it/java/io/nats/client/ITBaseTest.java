// Copyright 2017-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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