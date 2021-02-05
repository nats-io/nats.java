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

import io.nats.client.*;
import io.nats.client.NatsServerProtocolMock.ExitAt;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static io.nats.client.utils.TestBase.standardConnectionWait;
import static org.junit.jupiter.api.Assertions.*;

public class NatsMessageTests {
    @Test
    public void testSizeOnProtocolMessage() {
        NatsMessage msg = new NatsMessage.ProtocolMessage("PING");

        assertEquals(msg.getProtocolBytes().length + 2, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals("PING".getBytes(StandardCharsets.UTF_8).length + 2, msg.getSizeInBytes(), "Size is correct");
    }
    
    @Test
    public void testSizeOnPublishMessage() {
        byte[] body = new byte[10];
        String subject = "subj";
        String replyTo = "reply";
        String protocol = "PUB "+subject+" "+replyTo+" "+body.length;

        NatsMessage msg = new NatsMessage(subject, replyTo, body, false);

        assertEquals(msg.getProtocolBytes().length + body.length + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.US_ASCII).length + body.length + 4, msg.getSizeInBytes(), "Size is correct");
    
        msg = new NatsMessage(subject, replyTo, body, true);

        assertEquals(msg.getProtocolBytes().length + body.length + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.UTF_8).length + body.length + 4, msg.getSizeInBytes(), "Size is correct");
    }
    
    @Test
    public void testCustomMaxControlLine() {
        assertThrows(IllegalArgumentException.class, () -> {
            byte[] body = new byte[10];
            String subject = "subject";
            int maxControlLine = 1024;

            while (subject.length() <= maxControlLine) {
                subject += subject;
            }

            try (NatsTestServer ts = new NatsTestServer()) {
                Options options = new Options.Builder().
                            server(ts.getURI()).
                            maxReconnects(0).
                            maxControlLine(maxControlLine).
                            build();
                Connection nc = Nats.connect(options);
                standardConnectionWait(nc);
                nc.request(subject, body);
            }
        });
    }
    
    @Test
    public void testBigProtocolLineWithoutBody() {
        assertThrows(IllegalArgumentException.class, () -> {
            String subject = "subject";

            while (subject.length() <= Options.DEFAULT_MAX_CONTROL_LINE) {
                subject += subject;
            }

            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT);
                        NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);
                nc.subscribe(subject);
            }
        });
    }
    
    @Test
    public void testBigProtocolLineWithBody() {
        assertThrows(IllegalArgumentException.class, () -> {
            byte[] body = new byte[10];
            String subject = "subject";
            String replyTo = "reply";

            while (subject.length() <= Options.DEFAULT_MAX_CONTROL_LINE) {
                subject += subject;
            }

            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT);
                        NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);
                nc.publish(subject, replyTo, body);
            }
        });
    }

    @Test
    public void miscCoverage() {
        NatsMessage m = testMessage();
        String ms = m.toString();
        assertNotNull(ms);
        assertTrue(ms.contains("subject='test'"));

        assertNotNull(m.getHeaders());
        assertTrue(m.isUtf8mode());
        assertFalse(m.getHeaders().isEmpty());
        assertNull(m.getSubscription());
        assertNull(m.getNatsSubscription());
        assertNull(m.getConnection());
        assertEquals(23, m.getControlLineLength());

        m.headers = null; // we can do this because we have package access
        m.dirty = true; // for later tests, also is true b/c we nerfed the headers
        assertNull(m.getHeaders());

        assertNotNull(m.getOrCreateHeaders());

        NatsMessage.ProtocolMessage pm = new NatsMessage.ProtocolMessage((byte[])null);
        assertNotNull(pm.protocolBytes);
        assertEquals(0, pm.protocolBytes.length);
    }

    @Test
    public void constructorWithMessage() {
        NatsMessage m = testMessage();

        NatsMessage copy = new NatsMessage(m);
        assertEquals(m.getSubject(), copy.getSubject());
        assertEquals(m.getReplyTo(), copy.getReplyTo());
        assertEquals(m.getData(), copy.getData());
        assertEquals(m.getSubject(), copy.getSubject());
        assertEquals(m.getSubject(), copy.getSubject());
    }

    private NatsMessage testMessage() {
        Headers h = new Headers();
        h.add("key", "value");

        return NatsMessage.builder()
                .subject("test").replyTo("reply").headers(h).utf8mode(true)
                .data("data", StandardCharsets.US_ASCII)
                .build();
    }
}