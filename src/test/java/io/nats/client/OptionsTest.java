/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;
import java.util.List;

@Category(UnitTest.class)
public class OptionsTest {
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
    public void testGetUrl() {
        String url = "nats://localhost:1234";
        Options opts = new Options();
        assertEquals(null, opts.getUrl());
        opts.setUrl(url);
        String uriString = opts.getUrl().toString();
        assertEquals(url, uriString);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetUrlURI() {
        String urlString = "nats://localhost:1234";
        String badUrlString = "nats://larry:one:two@localhost:5151";

        Options opts = new Options();
        URI url = URI.create(urlString);
        opts.setUrl(url);
        assertEquals(url, opts.getUrl());

        try {
            url = URI.create(badUrlString);
        } catch (Exception e) {
            fail("Shouldn't throw exception here");
        }
        // This should thrown an IllegalArgumentException due to malformed
        // user info in the URL
        opts.setUrl(url);
    }

    @Test
    public void testSetUrlString() {
        final String urlString = "nats://localhost:1234";
        Options opts = new Options();
        assertEquals(null, opts.getUrl());

        opts.setUrl("");
        assertNull("Should be null", opts.getUrl());

        opts.setUrl((String) null);
        assertNull("Should be null", opts.getUrl());

        opts.setUrl(urlString);
        String uriString = opts.getUrl().toString();
        assertEquals(urlString, uriString);



        String badUrlString = "tcp://foobar baz";
        boolean exThrown = false;
        try {
            opts.setUrl(badUrlString);
        } catch (IllegalArgumentException e) {
            exThrown = true;
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            assertTrue("Should have thrown IllegalArgumentException", exThrown);
        }


    }

    @Test
    public void testGetHost() {
        URI uri = URI.create("nats://somehost:5555");
        Options opts = new Options();
        opts.setUrl(uri);
        assertEquals(uri.getHost(), opts.getHost());
    }

    // @Test
    // public void testSetHost() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    @Test
    public void testGetPort() {
        URI uri = URI.create("nats://somehost:5555");
        Options opts = new Options();
        opts.setUrl(uri);
        assertEquals(uri.getPort(), opts.getPort());
    }

    // @Test
    // public void testSetPort() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    @Test
    public void testGetUsername() {
        URI uri = URI.create("nats://larry:foobar@somehost:5555");
        Options opts = new Options();
        opts.setUrl(uri);
        String[] userTokens = uri.getUserInfo().split(":");
        String username = userTokens[0];
        assertEquals(username, opts.getUsername());
    }

    @Test
    public void testSetUsername() {
        String username = "larry";
        Options opts = new Options();
        opts.setUsername(username);
        assertEquals(username, opts.getUsername());
    }

    @Test
    public void testGetPassword() {
        assertNull(new Options().getPassword());
    }

    // @Test
    // public void testSetPassword() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetServers() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    @Test
    public void testSetServersStringArray() {
        String[] serverArray = { "nats://cluster-host-1:5151", "nats://cluster-host-2:5252" };
        List<URI> sList = ConnectionFactoryTest.serverArrayToList(serverArray);
        Options opts = new Options();
        opts.setServers(serverArray);
        assertEquals(sList, opts.getServers());

        String[] badServerArray = { "nats:// cluster-host-1:5151", "nats:// cluster-host-2:5252" };
        boolean exThrown = false;
        try {
            opts.setServers(badServerArray);
        } catch (IllegalArgumentException e) {
            exThrown = true;
        } finally {
            assertTrue("Should have thrown IllegalArgumentException", exThrown);
        }
    }

    // @Test
    // public void testSetServersListOfURI() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testIsNoRandomize() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetNoRandomize() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetConnectionName() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetConnectionName() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testIsVerbose() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetVerbose() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testIsPedantic() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetPedantic() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testIsSecure() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetSecure() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    @Test
    public void testIsTlsDebug() {
        Options opts = new Options();
        opts.setTlsDebug(true);
        assertTrue(opts.isTlsDebug());
    }

    // @Test
    // public void testSetTlsDebug() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    //
    // @Test
    // public void testIsReconnectAllowed() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetReconnectAllowed() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetMaxReconnect() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetMaxReconnect() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetReconnectWait() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetReconnectWait() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetConnectionTimeout() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetConnectionTimeout() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetPingInterval() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetPingInterval() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetMaxPingsOut() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetMaxPingsOut() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetExceptionHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetExceptionHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetClosedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetClosedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetReconnectedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetReconnectedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetDisconnectedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetDisconnectedEventHandler() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetSslContext() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetSslContext() {
    // fail("Not yet implemented"); // TODO
    // }

}
