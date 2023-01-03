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

import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

public class ServerInfoTests {
    static byte[] nonce = "abcdefg".getBytes(StandardCharsets.UTF_8);
    static String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(nonce);
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
        ServerInfo info = new ServerInfo(json);

        ServerInfo info234 = new ServerInfo(json.replace("1.2.3", "2.3.4"));
        ServerInfo info235 = new ServerInfo(json.replace("1.2.3", "2.3.5"));
        ServerInfo info235Beta2 = new ServerInfo(json.replace("1.2.3", "2.3.5-beta.2"));
        assertTrue(info.isOlderThanVersion("2.3.4"));
        assertTrue(info234.isOlderThanVersion("2.3.5"));
        assertTrue(info235.isNewerVersionThan("2.3.5-beta.2"));
        assertTrue(info.isSameVersion("1.2.3"));
        assertTrue(info234.isSameVersion("2.3.4"));
        assertTrue(info235.isSameVersion("2.3.5"));
        assertTrue(info235Beta2.isSameVersion("2.3.5-beta.2"));
        assertFalse(info235.isSameVersion("2.3.4"));
        assertFalse(info235Beta2.isSameVersion("2.3.5"));
        assertTrue(info234.isNewerVersionThan("1.2.3"));
        assertTrue(info235.isNewerVersionThan("2.3.4"));
        assertTrue(info235Beta2.isOlderThanVersion("2.3.5"));

        assertTrue(info234.isNewerVersionThan("not-a-number"));
        assertFalse(info234.isNewerVersionThan("2.3.5"));
        assertFalse(info235.isOlderThanVersion("2.3.4"));

        assertFalse(info235.isSameOrOlderThanVersion("2.3.4"));
        assertFalse(info235Beta2.isSameOrOlderThanVersion("2.3.4"));
        assertTrue(info234.isSameOrOlderThanVersion("2.3.4"));
        assertTrue(info.isSameOrOlderThanVersion("2.3.4"));

        assertTrue(info235.isSameOrNewerThanVersion("2.3.4"));
        assertTrue(info235Beta2.isSameOrNewerThanVersion("2.3.4"));
        assertTrue(info234.isSameOrNewerThanVersion("2.3.4"));
        assertFalse(info.isSameOrNewerThanVersion("2.3.4"));
        assertFalse(info234.isSameOrNewerThanVersion("2.3.5-beta.2"));

        ServerInfo info2310 = new ServerInfo(json.replace("1.2.3", "2.3.10"));
        ServerInfo info2103 = new ServerInfo(json.replace("1.2.3", "2.10.3"));
        assertTrue(info235.isOlderThanVersion("2.3.10"));
        assertTrue(info235.isOlderThanVersion("2.10.3"));
        assertTrue(info2310.isNewerVersionThan("2.3.5"));
        assertTrue(info2310.isOlderThanVersion("2.10.3"));
        assertTrue(info2103.isNewerVersionThan("2.3.5"));
        assertTrue(info2103.isNewerVersionThan("2.3.10"));

        ServerInfo infoAlpha1 = new ServerInfo(json.replace("1.2.3", "1.0.0-alpha1"));
        ServerInfo infoAlpha2 = new ServerInfo(json.replace("1.2.3", "1.0.0-alpha2"));
        ServerInfo infoBeta1 = new ServerInfo(json.replace("1.2.3", "1.0.0-beta1"));

        assertTrue(infoAlpha1.isOlderThanVersion("1.0.0-alpha2"));
        assertTrue(infoAlpha1.isOlderThanVersion("1.0.0-beta1"));
        assertTrue(infoAlpha2.isNewerVersionThan("1.0.0-alpha1"));
        assertTrue(infoAlpha2.isOlderThanVersion("1.0.0-beta1"));
        assertTrue(infoBeta1.isNewerVersionThan("1.0.0-alpha1"));
        assertTrue(infoBeta1.isNewerVersionThan("1.0.0-alpha2"));
    }

    @Test
    public void testEmptyURLParsing() {
        String json = "INFO {" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"connect_urls\":[\"one\", \"\"]" +
                       "}";
        ServerInfo info = new ServerInfo(json);
        assertEquals(info.getServerId(), "myserver");
        assertEquals(1, info.getConnectURLs().size());
        assertEquals("one", info.getConnectURLs().get(0));
    }

    @Test
    public void testIPV6InBrackets() {
        String json = "INFO {" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"connect_urls\":[\"one:4222\", \"[a:b:c]:4222\", \"[d:e:f]:4223\"]" + "," +
                        "\"max_payload\":100000000000" +
                       "}";
        ServerInfo info = new ServerInfo(json);
        assertEquals(info.getServerId(), "myserver");
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
        assertEquals(info.getServerId(), "myserver");
        assertEquals(info.getVersion(), "??????");
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

    @Test
    public void testInvalidUnicode() {
        String json = "INFO {\"server_id\":\"\\"+"u33"+"\"}";
        ServerInfo info = new ServerInfo(json);
        assertEquals("u33", info.getServerId());
    }
}