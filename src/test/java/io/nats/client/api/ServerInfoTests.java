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

package io.nats.client.api;

import io.nats.client.support.ServerVersion;
import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static io.nats.client.support.Encoding.base64UrlEncodeToString;
import static org.junit.jupiter.api.Assertions.*;

public class ServerInfoTests {
    static byte[] nonce = "abcdefg".getBytes(StandardCharsets.UTF_8);
    static String encoded = base64UrlEncodeToString(nonce);
    static String json = ResourceUtils.dataAsString("ServerInfoJson.txt").replace("<encoded>", encoded);

    @Test
    public void testValidInfoString() {
        byte[] ascii = encoded.getBytes(StandardCharsets.US_ASCII);

        ServerInfo info = new ServerInfo(json);
        assertEquals("serverId", info.getServerId());
        assertEquals("serverName", info.getServerName());
        assertEquals("1.2.3", info.getVersion());
        assertEquals("go0.0.0", info.getGoVersion());
        assertEquals("host", info.getHost());
        assertEquals(7777, info.getPort());
        assertTrue(info.isAuthRequired());
        assertTrue(info.isTLSRequired());
        assertTrue(info.isTLSAvailable());
        assertTrue(info.isHeadersSupported());
        assertEquals(100_000_000_000L, info.getMaxPayload());
        assertEquals(1, info.getProtocolVersion());
        assertTrue(info.isLameDuckMode());
        assertTrue(info.isJetStreamAvailable());
        assertEquals(42, info.getClientId());
        assertEquals("127.0.0.1", info.getClientIp());
        assertEquals("cluster", info.getCluster());
        assertEquals(2, info.getConnectURLs().size());
        assertEquals("url0", info.getConnectURLs().get(0));
        assertEquals("url1", info.getConnectURLs().get(1));
        assertArrayEquals(ascii, info.getNonce());

        assertNotNull(info.toString()); // COVERAGE
    }

    @Test
    public void testServerVersionComparisonsWork() {
        ServerInfo si234 = new ServerInfo(json.replace("1.2.3", "2.3.4"));
        ServerInfo si235A1 = new ServerInfo(json.replace("1.2.3", "2.3.5-alpha.1"));
        ServerInfo si235A2 = new ServerInfo(json.replace("1.2.3", "2.3.5-alpha-2"));
        ServerInfo si235B1 = new ServerInfo(json.replace("1.2.3", "v2.3.5-beta.1"));
        ServerInfo si235B2 = new ServerInfo(json.replace("1.2.3", "v2.3.5-beta-2"));
        ServerInfo si235 = new ServerInfo(json.replace("1.2.3", "2.3.5"));

        ServerInfo[] infos = new ServerInfo[]{si234, si235A1, si235A2, si235B1, si235B2, si235};
        for (int i = 0; i < infos.length; i++) {
            ServerInfo si = infos[i];
            for (int j = 0; j < infos.length; j++) {
                String v2 = new ServerVersion(infos[j].getVersion()).toString();
                if (i == j) {
                    assertTrue(si.isSameVersion(v2));
                    assertTrue(si.isSameOrOlderThanVersion(v2));
                    assertTrue(si.isSameOrNewerThanVersion(v2));
                    assertFalse(si.isNewerVersionThan(v2));
                    assertFalse(si.isOlderThanVersion(v2));
                }
                else {
                    assertFalse(si.isSameVersion(v2));
                    if (i < j) {
                        assertTrue(si.isOlderThanVersion(v2));
                        assertTrue(si.isSameOrOlderThanVersion(v2));
                        assertFalse(si.isNewerVersionThan(v2));
                        assertFalse(si.isSameOrNewerThanVersion(v2));
                    }
                    else { // i > j
                        assertFalse(si.isOlderThanVersion(v2));
                        assertFalse(si.isSameOrOlderThanVersion(v2));
                        assertTrue(si.isNewerVersionThan(v2));
                        assertTrue(si.isSameOrNewerThanVersion(v2));
                    }
                }
            }
        }

        assertTrue(si234.isNewerVersionThan("not-a-number.2.3"));
        assertTrue(si234.isNewerVersionThan("1.not-a-number.3"));
        assertTrue(si234.isNewerVersionThan("1.2.not-a-number"));
        assertTrue(si234.isNewerVersionThan("2.3"));
        assertFalse(si234.isOlderThanVersion("2.3"));
        assertTrue(si235A1.isNewerVersionThan("2.3"));
        assertFalse(si235A1.isOlderThanVersion("2.3"));

        ServerInfo siPadded1 = new ServerInfo(json.replace("1.2.3", "1.20.30"));
        ServerInfo siPadded2 = new ServerInfo(json.replace("1.2.3", "40.500.6000"));
        assertTrue(siPadded1.isSameVersion("1.20.30"));
        assertTrue(siPadded2.isSameVersion("40.500.6000"));
        assertTrue(siPadded2.isNewerVersionThan(siPadded1.getVersion()));
        assertTrue(siPadded1.isOlderThanVersion(siPadded2.getVersion()));
    }

    @Test
    public void testEmptyURLParsing() {
        String json = "INFO {" +
            "\"server_id\":\"myserver\"" + "," +
            "\"connect_urls\":[\"one\", \"\"]" +
            "}";
        ServerInfo info = new ServerInfo(json);
        assertEquals("myserver", info.getServerId());
        assertEquals(1, info.getConnectURLs().size());
        assertEquals("one", info.getConnectURLs().get(0));

        json = "INFO {\"server_id\":\"myserver\"}";
        info = new ServerInfo(json);
        assertNotNull(info.getConnectURLs());
        assertEquals(0, info.getConnectURLs().size());
    }

    @Test
    public void testIPV6InBrackets() {
        String json = "INFO {" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"connect_urls\":[\"one:4222\", \"[a:b:c]:4222\", \"[d:e:f]:4223\"]" + "," +
                        "\"max_payload\":100000000000" +
                       "}";
        ServerInfo info = new ServerInfo(json);
        assertEquals("myserver", info.getServerId());
        assertEquals(3, info.getConnectURLs().size());
        assertEquals("one:4222", info.getConnectURLs().get(0));
        assertEquals("[a:b:c]:4222", info.getConnectURLs().get(1));
        assertEquals("[d:e:f]:4223", info.getConnectURLs().get(2));
    }

    @Test
    public void testInvalid() {
        assertThrows(IllegalArgumentException.class, () -> new ServerInfo(null));
        assertThrows(IllegalArgumentException.class, () -> new ServerInfo(""));
        assertThrows(IllegalArgumentException.class, () -> new ServerInfo("invalid}"));
    }

    @Test
    public void testNonAsciiValue() {
        String json = "INFO {" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"version\":\"??????\"" +
                       "}";
        ServerInfo info = new ServerInfo(json);
        assertEquals("myserver", info.getServerId());
        assertEquals("??????", info.getVersion());
    }

    @Test
    public void testEncodingInString() {
        String json = "INFO {" +
                        "\"server_id\":\"\\\\\\b\\f\\n\\r\\t\"" + "," +
                        "\"go\":\"my\\u0021server\"" + "," +
                        "\"host\":\"my\\\\host\"" + "," +
                        "\"version\":\"1.1.1\\t1\"" +
                       "}";
        ServerInfo info = new ServerInfo(json);
        assertEquals("\\\b\f\n\r\t", info.getServerId());
        assertEquals("my!server", info.getGoVersion());
        assertEquals("my\\host", info.getHost());
        assertEquals("1.1.1\t1", info.getVersion());
    }
}