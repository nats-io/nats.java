// Copyright 2015-2018 The NATS Authors
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(UnitTest.class)
public class ServerInfoTest extends BaseUnitTest {

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
        assertTrue(s2.hashCode() == s1.hashCode());
    }

    @Test
    public void testCompareNull() {
        assertFalse(ServerInfo.compare("foo", "bar"));
        assertFalse(ServerInfo.compare(null, "bar"));
        assertFalse(ServerInfo.compare("foo", null));
        assertTrue(ServerInfo.compare("foo", "foo"));
        assertTrue(ServerInfo.compare(null, null));
    }

    @Test
    public void testEqualsFailure() {
        ServerInfo s1 = ServerInfo.createFromWire(testString);
        assertFalse(s1.equals(null));
        assertFalse(s1.equals("foo"));

        ServerInfo s2 = null;

        s2 = new ServerInfo(s1);
        s2.setId("foo");
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setVersion("9.1.5000");
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setGoVersion("14.1");
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setHost("s2hostname");
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setPort(5101);
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setAuthRequired(!s1.isAuthRequired());
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setAuthRequired(!s1.isAuthRequired());
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setSslRequired(!s1.isSslRequired());
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setTlsRequired(!s1.isTlsRequired());
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setTlsVerify(!s1.isTlsVerify());
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setMaxPayload(2 * 1024 * 1024);
        assertFalse(s2.equals(s1));

        s2 = new ServerInfo(s1);
        s2.setConnectUrls(new String[] { "foo", "bar"});
        assertFalse(s2.equals(s1));
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

    @Test
    public void testIsSslRequired() {
        assertEquals(true, testInstance.isSslRequired());
        testInstance.setSslRequired(false);
        assertEquals(false, testInstance.isSslRequired());
        testInstance.setSslRequired(true);
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

    @Test
    public void testIsTlsVerify() {
        assertEquals(false, testInstance.isTlsVerify());
        testInstance.setTlsVerify(true);
        assertEquals(true, testInstance.isTlsVerify());
        testInstance.setTlsVerify(false);
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
