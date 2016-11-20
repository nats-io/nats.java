/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category(UnitTest.class)
public class ServerInfoTest {
    final Logger logger = LoggerFactory.getLogger(ServerInfoTest.class);

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    private static final String testString =
            "INFO {\"server_id\":\"s76hOxUCzhR2ngkcVYSPPV\",\"version\":\"0.9.4\","
                    + "\"go\":\"go1.6.3\",\"host\":\"0.0.0.0\",\"port\":4222,"
                    + "\"auth_required\":true,\"ssl_required\":true,\"tls_required\":true,"
                    + "\"tls_verify\":false,\"max_payload\":1048576,"
                    + "\"connect_urls\":[\"10.0.1.3:4222\",\"[fe80::42:aff:fe00:103]:4222\"]}\r\n";
    private static final String testStringNoConnectedUrls =
            "INFO {\"server_id\":\"s76hOxUCzhR2ngkcVYSPPV\",\"version\":\"0.9.4\","
                    + "\"go\":\"go1.6.3\",\"host\":\"0.0.0.0\",\"port\":4222,"
                    + "\"auth_required\":true,\"ssl_required\":true,\"tls_required\":true,"
                    + "\"tls_verify\":false,\"max_payload\":1048576}\r\n";
    private static final ServerInfo testInstance = ServerInfo.createFromWire(testString);

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

    /**
     * Test method for {@link io.nats.client.ServerInfo#ServerInfo(io.nats.client.ServerInfo)}.
     */
    @Test
    public void testServerInfoCopyConstructor() {
        ServerInfo s1 = ServerInfo.createFromWire(testString);
        ServerInfo s2 = new ServerInfo(s1);
        assertTrue(EqualsBuilder.reflectionEquals(s1, s2));

        s1 = ServerInfo.createFromWire(testStringNoConnectedUrls);
        s2 = new ServerInfo(s1);
        assertTrue(EqualsBuilder.reflectionEquals(s1, s2));

        assertTrue(s2.equals(s1));
    }

    @Test
    public void testGetConnectUrls() {
        ServerInfo info = new ServerInfo();
        info.setConnectUrls(testInstance.getConnectUrls());
        assertArrayEquals(new String[] { "10.0.1.3:4222" , "[fe80::42:aff:fe00:103]:4222" }, testInstance.getConnectUrls());
        assertArrayEquals(testInstance.getConnectUrls(), info.getConnectUrls());
    }

    /**
     * Test method for {@link io.nats.client.ServerInfo#getId()}.
     */
    @Test
    public void testGetId() {
        ServerInfo serverInfo = new ServerInfo();
        serverInfo.setId(testInstance.getId());
        assertEquals("s76hOxUCzhR2ngkcVYSPPV", testInstance.getId());
        assertEquals(testInstance.getId(), serverInfo.getId());
    }

    /**
     * Test method for {@link io.nats.client.ServerInfo#getHost()}.
     */
    @Test
    public void testGetHost() {
        ServerInfo serverInfo = new ServerInfo();
        serverInfo.setHost(testInstance.getHost());
        assertEquals("0.0.0.0", testInstance.getHost());
        assertEquals(testInstance.getHost(), serverInfo.getHost());
    }

    /**
     * Test method for {@link io.nats.client.ServerInfo#getPort()}.
     */
    @Test
    public void testGetPort() {
        ServerInfo info = new ServerInfo();
        info.setPort(testInstance.getPort());
        assertEquals(4222, testInstance.getPort());
        assertEquals(testInstance.getPort(), info.getPort());
    }

    /**
     * Test method for {@link io.nats.client.ServerInfo#getVersion()}.
     */
    @Test
    public void testGetVersion() {
        ServerInfo info = new ServerInfo();
        info.setVersion(testInstance.getVersion());
        assertEquals("0.9.4", testInstance.getVersion());
        assertEquals(testInstance.getVersion(), info.getVersion());
    }

    /**
     * Test method for {@link io.nats.client.ServerInfo#isAuthRequired()}.
     */
    @Test
    public void testIsAuthRequired() {
        assertEquals(true, testInstance.isAuthRequired());
        testInstance.setAuthRequired(false);
        assertEquals(false, testInstance.isAuthRequired());
        testInstance.setAuthRequired(true);
    }

    /**
     * Test method for {@link io.nats.client.ServerInfo#isTlsRequired()}.
     */
    @Test
    public void testIsTlsRequired() {
        assertEquals(true, testInstance.isTlsRequired());
        testInstance.setTlsRequired(false);
        assertEquals(false, testInstance.isTlsRequired());
        testInstance.setTlsRequired(true);
    }

    /**
     * Test method for {@link io.nats.client.ServerInfo#getMaxPayload()}.
     */
    @Test
    public void testGetMaxPayload() {
        assertEquals(1048576, testInstance.getMaxPayload());
        testInstance.setMaxPayload(123456);
        assertEquals(123456, testInstance.getMaxPayload());
        testInstance.setMaxPayload(1048576);
    }

    /**
     * Test method for {@link io.nats.client.ServerInfo#toString()}.
     */
    @Test
    public void testToString() {
        String outputString = testInstance.toString();
        assertEquals(testString.trim(), outputString);
    }
}
