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
                        "\"ssl_required\": true" + "," +
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
        assertEquals(info.isSSLRequired(), true);

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
    public void testBadStringKey() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"versionitis\":\"1.1.1\"" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getUnknownInfo().size(), 1);
    }

    @Test
    public void testBadStringValue() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"version\":222" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getUnknownInfo().size(), 1);
    }

    @Test
    public void testBadLongKey() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"max_payloads\":100000000000" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getUnknownInfo().size(), 1);
    }

    @Test
    public void testBadBooleanKey() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"auth_is_required\":false" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getUnknownInfo().size(), 1);
    }

    @Test
    public void testBadArrayKey() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"connect_urls_list\":[\"one\", \"two\"]" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getUnknownInfo().size(), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBadArrayContents() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"connect_urls\":[\"one\", \"two\", 3]" +
                       "}";
        new NatsServerInfo(json);
    }

    @Test
    public void testEncodingInString() {
        String json = "{" +
                        "\"server_id\":\"my\\tserver\"" + "," +
                        "\"go\":\"my\\u0021server\"" + "," +
                        "\"host\":\"my\\\\host\"" + "," +
                        "\"version\":\"1.1.1\\\"1\"" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getServerId(), "my\tserver");
        assertEquals(info.getGoVersion(), "my!server");
        assertEquals(info.getHost(), "my\\host");
        assertEquals(info.getVersion(), "1.1.1\"1");
    }

    @Test
    public void testNegativeLong() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"max_payload\":-100000000000" +
                       "}";
        NatsServerInfo info = new NatsServerInfo(json);
        assertEquals(info.getServerId(), "myserver");
        assertEquals(info.getMaxPayload(), -100_000_000_000L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDouble() {
        String json = "{" +
                        "\"server_id\":\"myserver\"" + "," +
                        "\"max_payload\":1.1" +
                       "}";
        new NatsServerInfo(json);
    }
}