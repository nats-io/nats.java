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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class NatsServerInfoTests {
    @Test
    public void testValidInfoString() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"version\":\"1.1.1\"" + "," +
                        "\"go\": \"go1.9\"" + "," +
                        "\"host\": \"host\"" + "," +
                        "\"tls_required\": true" + "," +
                        "\"auth_required\":false" + "," +
                        "\"port\": 7777" + "," +
                        "\"max_payload\":100000000000" + "," +
                        "\"connect_urls\":[\"one\", \"two\"]" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getServerId(), "myserver");
        assertEquals(info.getVersion(), "1.1.1");
        assertEquals(info.getGoVersion(), "go1.9");
        assertEquals(info.getHost(), "host");
        assertEquals(info.getPort(), 7777);
        assertEquals(info.getMaxPayload(), 100_000_000_000L);
        assertEquals(info.isAuthRequired(), false);
        assertEquals(info.isTLSRequired(), true);

        String[] urls = {"one", "two"};
        assertTrue(Arrays.equals(info.getConnectURLs(), urls));
    }

    @Test
    public void testNonAsciiValue() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"version\":\"世界\"" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getServerId(), "myserver");
        assertEquals(info.getVersion(), "世界");
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
}