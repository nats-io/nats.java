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

package io.nats.client.impl;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

public class NatsServerInfoTests {
    @Test
    public void testValidInfoString() {
        byte[] nonce = "abcdefg".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(nonce);
        byte[] ascii = encoded.getBytes(StandardCharsets.US_ASCII);
        String[] urls = {"url1", "url2"};

        String json = rawJson.replace("<encoded>", encoded);

        NatsServerInfo info = new NatsServerInfo(json);
        _testValid(ascii, urls, info);

        info = new NatsServerInfo(info.toString().replaceAll("\n", ""));
        _testValid(ascii, urls, info);

        assertThrows(IllegalArgumentException.class, () -> new NatsServerInfo(""));

        // just extra pathways, all fields won't be found
        new NatsServerInfo("{\"foo\":42}");
    }

    private void _testValid(byte[] ascii, String[] urls, NatsServerInfo info) {
        assertEquals("serverId", info.getServerId());
        assertEquals("serverName", info.getServerName());
        assertEquals("0.0.0", info.getVersion());
        assertEquals("go0.0.0", info.getGoVersion());
        assertEquals("host", info.getHost());
        assertEquals(7777, info.getPort());
        assertFalse(info.isAuthRequired());
        assertTrue(info.isTLSRequired());
        assertEquals(100_000_000_000L, info.getMaxPayload());
        assertEquals(1, info.getProtocolVersion());
        assertFalse(info.isLameDuckMode());
        assertTrue(info.isJetStreamAvailable());
        assertEquals(42, info.getClientId());
        assertEquals("127.0.0.1", info.getClientIp());
        assertEquals("cluster", info.getCluster());
        assertArrayEquals(urls, info.getConnectURLs());
        assertArrayEquals(ascii, info.getNonce());
    }

    static String rawJson = "{" +
            "\"server_id\": \"serverId\"," +
            "\"server_name\": \"serverName\"," +
            "\"version\": \"0.0.0\"," +
            "\"go\": \"go0.0.0\"," +
            "\"host\": \"host\"," +
            "\"port\": 7777," +
            "\"headersSupported\": true," +
            "\"auth_required\": false," +
            "\"tls_required\": true," +
            "\"max_payload\": 100000000000," +
            "\"proto\": 1," +
            "\"ldm\": false," +
            "\"jetstream\": true," +
            "\"client_id\": 42," +
            "\"client_ip\": \"127.0.0.1\"" +
            "\"cluster\": \"cluster\"" +
            "\"connect_urls\":[\"url1\", \"url2\"]" +
            "\"nonce\":\"<encoded>\"" +
            "}";

    @Test
    public void testEmptyURLParsing() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"connect_urls\":[\"one\", \"\"]" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getServerId(), "myserver");
        String[] urls = {"one"};
        assertTrue(Arrays.equals(info.getConnectURLs(), urls));
    }

    @Test
    public void testIPV6InBrackets() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"connect_urls\":[\"one:4222\", \"[a:b:c]:4222\", \"[d:e:f]:4223\"]" + "," +
                        "\"max_payload\":100000000000" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getServerId(), "myserver");
        String[] urls = {"one:4222", "[a:b:c]:4222", "[d:e:f]:4223"};
        assertTrue(Arrays.equals(info.getConnectURLs(), urls));
    }
    
    @Test
    public void testThrowsOnNonJson() {
        assertThrows(IllegalArgumentException.class, () -> {
            String json = "foo";
            new NatsServerInfo(json);
            assertFalse(true);
        });
    }
       
    
    @Test
    public void testThrowsOnShortString() {
        assertThrows(IllegalArgumentException.class, () -> {
            String json = "{}";
            new NatsServerInfo(json);
            assertFalse(true);
        });
    }

    @Test
    public void testNonAsciiValue() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"version\":\"??????\"" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getServerId(), "myserver");
        assertEquals(info.getVersion(), "??????");
    }

    @Test
    public void testEncodingInString() {
        String json = "{" +
                        "\"server_id\":\"\\\\\\b\\f\\n\\r\\t\"" + "," +
                        "\"go\":\"my\\u0021server\"" + "," +
                        "\"host\":\"my\\\\host\"" + "," +
                        "\"version\":\"1.1.1\\t1\"" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals("\\\b\f\n\r\t", info.getServerId());
        assertEquals("my!server", info.getGoVersion());
        assertEquals("my\\host", info.getHost());
        assertEquals("1.1.1\t1", info.getVersion());
    }

    @Test
    public void testInvalidUnicode() {
        String json = "{\"server_id\":\"\\"+"u33"+"\"}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals("u33", info.getServerId());
    }
}