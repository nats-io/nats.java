/*
 *  Copyright (c) 2015-2017 Apcera Inc. All rights reserved. This program and the accompanying materials are made available under the terms of the MIT License (MIT) which accompanies this distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.UnitTestUtilities.newMockedTcpConnectionFactory;
import static org.junit.Assert.*;

import java.net.URI;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
/**
 * Created by larry on 11/15/16.
 */
public class NatsTest extends BaseUnitTest {

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Test
    public void connectUrlOptions() throws Exception {
        TcpConnectionFactory tcf = newMockedTcpConnectionFactory();
        Options opts = new Options.Builder().factory(tcf).build();
        try (Connection conn = Nats.connect("nats://foobar:2222", opts)) {

        }
    }

    @Test
    public void defaultOptions() throws Exception {
        Options opts = Nats.defaultOptions();
        assertNotNull(opts);
        assertNull(opts.getUrl());
    }

    @Test
    public void processUrlString() throws Exception {
        String urls = "nats://localhost:4222, nats://anotherhost:2222";
        List<URI> uriList = Nats.processUrlString(urls);
        assertEquals(2, uriList.size());
        assertEquals("nats://localhost:4222", uriList.get(0).toString());
        assertEquals("nats://anotherhost:2222", uriList.get(1).toString());
    }

    @Test
    public void processUrlArray() throws Exception {
        String[] urls = { "nats://localhost:4222", "nats://anotherhost:2222" };
        List<URI> uriList = Nats.processUrlArray(urls);
        assertEquals(2, uriList.size());
        assertEquals("nats://localhost:4222", uriList.get(0).toString());
        assertEquals("nats://anotherhost:2222", uriList.get(1).toString());
    }


    @Test
    public void testMsgDeliveryThreadPool() {
        boolean ok = false;
        try { Nats.createMsgDeliveryThreadPool(-1); } catch (IllegalArgumentException e) { ok = true; }
        assertTrue(ok);
        assertEquals(0, Nats.getMsgDeliveryThreadPoolSize());

        Nats.createMsgDeliveryThreadPool(3);
        int ps = Nats.getMsgDeliveryThreadPoolSize();
        assertEquals(3, ps);

        // Trying to create again should fail
        ok = false;
        try { Nats.createMsgDeliveryThreadPool(5); } catch (IllegalStateException e) { ok = true; }
        assertTrue(ok);
        // Should still be of size 3.
        assertEquals(3, Nats.getMsgDeliveryThreadPoolSize());

        // However, on shutdown, we want to clear the pool.
        Nats.shutdownMsgDeliveryThreadPool();
        ps = Nats.getMsgDeliveryThreadPoolSize();
        assertEquals(0, ps);

        // Restart pool
        Nats.createMsgDeliveryThreadPool(2);
        ps = Nats.getMsgDeliveryThreadPoolSize();
        assertEquals(2, ps);

        // Shutdown
        Nats.shutdownMsgDeliveryThreadPool();
        ps = Nats.getMsgDeliveryThreadPoolSize();
        assertEquals(0, ps);
        assertNull(Nats.getMsgDeliveryThreadPool());
    }
}