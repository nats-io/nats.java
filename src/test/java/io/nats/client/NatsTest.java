/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying materials are made available under the terms of the MIT License (MIT) which accompanies this distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.UnitTestUtilities.newMockedTcpConnectionFactory;
import static org.junit.Assert.*;

import java.net.URI;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by larry on 11/15/16.
 */
public class NatsTest {

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

//    @Test
//    public void connect() throws Exception {
//        Nats.connect();
//    }

//    @Test
//    public void connectUrl() throws Exception {
//        Nats.connect()
//    }

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

}