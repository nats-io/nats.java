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
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import io.nats.client.Nats;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;

public class ParseTests {
    @Test
    public void testGoodNumbers() {
        int i=1;

        while (i < 2_000_000_000 && i > 0) {
            assertEquals(i, NatsConnectionReader.parseLength(String.valueOf(i)));
            i *= 11;
        }

        assertEquals(0, NatsConnectionReader.parseLength("0"));

    }

    @Test(expected=NumberFormatException.class)
    public void testBadChars() {
        NatsConnectionReader.parseLength("2221a");
        assertFalse(true);
    }

    @Test(expected=NumberFormatException.class)
    public void testTooBig() {
        NatsConnectionReader.parseLength(String.valueOf(100_000_000_000L));
        assertFalse(true);
    }

    @Test(expected=IOException.class)
    public void testLongProtocolOpThrows() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            NatsConnectionReader reader = nc.getReader();
            byte[] bytes = ("thisistoolong\r\n").getBytes(StandardCharsets.US_ASCII);
            reader.fakeReadForTest(bytes);
            reader.gatherOp(bytes.length);
            assertFalse(true);
        }
    }

    @Test(expected=IOException.class)
    public void testMissingLineFeed() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            NatsConnectionReader reader = nc.getReader();
            byte[] bytes = ("PING\rPONG").getBytes(StandardCharsets.US_ASCII);
            reader.fakeReadForTest(bytes);
            reader.gatherOp(bytes.length);
            assertFalse(true);
        }
    }

    @Test(expected=IOException.class)
    public void testMissingSubject() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            NatsConnectionReader reader = nc.getReader();
            byte[] bytes = ("MSG  1 1\r\n").getBytes(StandardCharsets.US_ASCII);
            reader.fakeReadForTest(bytes);
            reader.gatherOp(bytes.length);
            reader.gatherMessageProtocol(bytes.length);
            reader.parseProtocolMessage();
            assertFalse(true);
        }
    }

    @Test(expected=IOException.class)
    public void testMissingSID() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            NatsConnectionReader reader = nc.getReader();
            byte[] bytes = ("MSG subject  1\r\n").getBytes(StandardCharsets.US_ASCII);
            reader.fakeReadForTest(bytes);
            reader.gatherOp(bytes.length);
            reader.gatherMessageProtocol(bytes.length);
            reader.parseProtocolMessage();
            assertFalse(true);
        }
    }

    @Test(expected=IOException.class)
    public void testMissingLength() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            NatsConnectionReader reader = nc.getReader();
            byte[] bytes = ("MSG subject 2 \r\n").getBytes(StandardCharsets.US_ASCII);
            reader.fakeReadForTest(bytes);
            reader.gatherOp(bytes.length);
            reader.gatherMessageProtocol(bytes.length);
            reader.parseProtocolMessage();
            assertFalse(true);
        }
    }

    @Test(expected=IOException.class)
    public void testBadLength() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            NatsConnectionReader reader = nc.getReader();
            byte[] bytes = ("MSG subject 2 x\r\n").getBytes(StandardCharsets.US_ASCII);
            reader.fakeReadForTest(bytes);
            reader.gatherOp(bytes.length);
            reader.gatherMessageProtocol(bytes.length);
            reader.parseProtocolMessage();
            assertFalse(true);
        }
    }

    @Test(expected=IOException.class)
    public void testMessageLineTooLong() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(new Options.Builder().
                                                                    server(ts.getURI()).
                                                                    maxControlLine(16).
                                                                    build())) {
            NatsConnectionReader reader = nc.getReader();
            byte[] bytes = ("MSG reallylongsubjectobreakthelength 1 1\r\n").getBytes(StandardCharsets.US_ASCII);
            reader.fakeReadForTest(bytes);
            reader.gatherOp(bytes.length);
            reader.gatherMessageProtocol(bytes.length);
            reader.parseProtocolMessage();
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testProtocolLineTooLong() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(new Options.Builder().
                                                                    server(ts.getURI()).
                                                                    maxControlLine(1024).
                                                                    build())) {
            NatsConnectionReader reader = nc.getReader();
            StringBuilder longString = new StringBuilder();

            longString.append("INFO ");
            for (int i=0;i<500;i++ ){
                longString.append("helloworld");
            }

            byte[] bytes = longString.toString().getBytes(StandardCharsets.US_ASCII);
            reader.fakeReadForTest(bytes);
            reader.gatherOp(bytes.length);
            reader.gatherProtocol(bytes.length);
            reader.parseProtocolMessage();
            assertFalse(true);
        }
    }

    @Test
    public void testProtocolStrings() throws Exception {
        String[] serverStrings = {
            "+OK", "PONG", "PING", "MSG longer.subject.abitlikeaninbox 22 longer.replyto.abitlikeaninbox 234",
            "-ERR some error with spaces in it", "INFO {" + "\"server_id\":\"myserver\"" + "," + "\"version\":\"1.1.1\"" + ","
            + "\"go\": \"go1.9\"" + "," + "\"host\": \"host\"" + "," + "\"tls_required\": true" + ","
            + "\"auth_required\":false" + "," + "\"port\": 7777" + "," + "\"max_payload\":100000000000" + ","
            + "\"connect_urls\":[\"one\", \"two\"]" + "}", "ping", "msg one 22 33", "+oK", "PoNg", "pong", "MsG one 22 23"
        };

        String[] badStrings = {
            "XXX", "XXXX", "XX", "X", "PINX", "PONX", "MSX", "INFX", "+OX", "-ERX",
            "xxx", "xxxx", "xx", "x", "pinx", "ponx", "msx", "infx", "+ox", "-erx",
            "+mk", "+ms", "-msg", "-esg", "poog", "piig", "mkg", "iing", "inng"
        };

        String[] expected = {
            NatsConnection.OP_OK, NatsConnection.OP_PONG, NatsConnection.OP_PING, NatsConnection.OP_MSG,
            NatsConnection.OP_ERR, NatsConnection.OP_INFO, NatsConnection.OP_PING, NatsConnection.OP_MSG,
            NatsConnection.OP_OK, NatsConnection.OP_PONG, NatsConnection.OP_PONG, NatsConnection.OP_MSG
        };

        try (NatsTestServer ts = new NatsTestServer(false);
                NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            NatsConnectionReader reader = nc.getReader();

            for (int i=0; i<serverStrings.length; i++) {
                byte[] bytes = (serverStrings[i]+"\r\n").getBytes(StandardCharsets.US_ASCII);
                reader.fakeReadForTest(bytes);
                reader.gatherOp(bytes.length);
                String op = reader.currentOp();
                assertEquals(serverStrings[i], expected[i], op);
            }

            for (int i=0; i<badStrings.length; i++) {
                byte[] bytes = (badStrings[i]+"\r\n").getBytes(StandardCharsets.US_ASCII);
                reader.fakeReadForTest(bytes);
                reader.gatherOp(bytes.length);
                String op = reader.currentOp();
                assertEquals(badStrings[i], NatsConnectionReader.UNKNOWN_OP, op);
            }
        }
    }
}