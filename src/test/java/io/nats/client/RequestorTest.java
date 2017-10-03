/*
 *  Copyright (c) 2015-2017 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.examples.Replier;
import io.nats.examples.Requestor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Category(IntegrationTest.class)
public class RequestorTest extends BaseUnitTest {

    final ExecutorService service = Executors.newCachedThreadPool();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testRequestorStringArray() throws Exception {
        List<String> argList = new ArrayList<String>();
        argList.addAll(Arrays.asList("-s", Nats.DEFAULT_URL));
        argList.addAll(Arrays.asList("foo", "Hello World!"));

        String[] args = new String[argList.size()];
        args = argList.toArray(args);

        try (NatsServer srv = runDefaultServer()) {
            new Requestor(args);
        }
    }

    @Test
    public void testParseArgsBadFlags() {
        List<String> argList = new ArrayList<String>();
        String[] flags = new String[] {"-s"};
        boolean exThrown = false;

        for (String flag : flags) {
            try {
                exThrown = false;
                argList.clear();
                argList.addAll(Arrays.asList(flag, "foo", "Hello World!"));
                String[] args = new String[argList.size()];
                args = argList.toArray(args);
                new Requestor(args);
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
        List<String> argList = new ArrayList<String>();
        boolean exThrown = false;

        try {
            exThrown = false;
            argList.clear();
            argList.addAll(Collections.singletonList("foo"));
            String[] args = new String[argList.size()];
            args = argList.toArray(args);
            new Requestor(args);
        } catch (IllegalArgumentException e) {
            assertEquals("must supply at least subject and msg", e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Should have thrown exception", exThrown);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMainFails() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            Requestor.main(new String[] {"foobar"});
        }
    }

    @Test(timeout = 5000)
    public void testMainSuccess() throws Exception {
        final List<Throwable> errors = new ArrayList<Throwable>();
        try (NatsServer srv = runDefaultServer()) {
            final CountDownLatch startPub = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(1);
            service.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Replier.main(new String[] {"-s", Nats.DEFAULT_URL, "-n", "1", "foo", "reply"});
                        done.countDown();
                    } catch (Exception e) {
                        errors.add(e);
                    }
                }
            });
            Thread.sleep(500);
            startPub.countDown();
            service.execute(new Runnable() {
                public void run() {
                    try {
                        startPub.await();
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

}
