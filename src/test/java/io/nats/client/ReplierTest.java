/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.examples.Replier;
import io.nats.examples.Requestor;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Category(IntegrationTest.class)
public class ReplierTest {

    private final ExecutorService service = Executors.newCachedThreadPool();

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

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
    public void testReplierStringArray() throws Exception {
        List<String> argList = new ArrayList<String>();
        argList.addAll(Arrays.asList("-s", Nats.DEFAULT_URL));
        argList.add("foo");
        argList.add("got it");

        String[] args = new String[argList.size()];
        args = argList.toArray(args);

        new Replier(args);
    }

    @Test
    public void testParseArgsBadFlags() {
        List<String> argList = new ArrayList<String>();
        String[] flags = new String[] {"-s", "--server", "-n", "--count", "-q", "--qgroup"};
        boolean exThrown = false;

        for (String flag : flags) {
            try {
                exThrown = false;
                argList.clear();
                argList.addAll(Arrays.asList(flag, "foo", "gotcha"));
                String[] args = new String[argList.size()];
                args = argList.toArray(args);
                new Replier(args);
            } catch (IllegalArgumentException e) {
                assertEquals(String.format("%s requires an argument", flag), e.getMessage());
                exThrown = true;
            } finally {
                assertTrue("Should have thrown exception", exThrown);
            }
        }
    }

    @Test
    public void testParseArgsNotEnoughArgs() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("must supply at least a subject name and a reply msg");

        List<String> argList = new ArrayList<String>();

        argList.clear();
        String[] args = new String[argList.size()];
        args = argList.toArray(args);
        new Replier(args);
    }

    @Test(timeout = 5000)
    public void testMainSuccess() throws Exception {
        final List<Throwable> errors = new ArrayList<Throwable>();
        try (NatsServer srv = runDefaultServer()) {
            final CountDownLatch startReq = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(1);
            service.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Replier.main(new String[] {"-s", Nats.DEFAULT_URL, "-n", "1", "foo",
                                "gotcha"});
                        done.countDown();
                    } catch (Exception e) {
                        errors.add(e);
                    }
                }
            });
            sleep(500);
            startReq.countDown();
            service.execute(new Runnable() {
                public void run() {
                    try {
                        startReq.await();
                        new Requestor(new String[] {"-s", Nats.DEFAULT_URL, "foo", "bar"}).run();
                    } catch (Exception e) {
                        errors.add(e);
                    }
                }
            });

            done.await();
            if (errors.size() != 0) {
                for (Throwable t : errors) {
                    t.printStackTrace();
                }
                fail("Unexpected exceptions");
            }
        }
    }

    @Test
    public void testMainFailsNoServers() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(Nats.ERR_NO_SERVERS);
        Replier.main(new String[] {"-s", "nats://enterprise:4242", "foobar", "gotcha"});
    }
}
