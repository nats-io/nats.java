/*
 *  Copyright (c) 2015-2017 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.nats.examples.Publisher;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Category(IntegrationTest.class)
public class PublisherTest extends BaseUnitTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testPublisherStringArray() throws Exception {
        List<String> argList = new ArrayList<String>();
        argList.addAll(Arrays.asList("-s", Nats.DEFAULT_URL));
        argList.addAll(Arrays.asList("foo", "Hello World!"));

        String[] args = new String[argList.size()];
        args = argList.toArray(args);

        try (NatsServer srv = runDefaultServer()) {
            new Publisher(args).run();
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
                new Publisher(args);
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
            new Publisher(args);
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
            Publisher.main(new String[] {"foobar"});
        }
    }

    @Test
    public void testMainSuccess() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            Publisher.main(new String[] {"-s", Nats.DEFAULT_URL, "foo", "bar"});
        }
    }

}
