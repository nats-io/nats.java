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
import io.nats.client.support.IncomingHeadersProcessor;
import io.nats.client.utils.ConnectionUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static io.nats.client.support.NatsConstants.OP_PING;
import static io.nats.client.support.NatsConstants.OP_PING_BYTES;
import static io.nats.client.utils.OptionsUtils.options;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static org.junit.jupiter.api.Assertions.*;

public class NatsMessageTests extends JetStreamTestBase {
    @Test
    public void testProtocolMessage() {
        NatsMessage msg = new ProtocolMessage(OP_PING_BYTES);
        assertEquals(msg.getProtocolBytes().length + 2, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(OP_PING_BYTES.length + 2, msg.getSizeInBytes(), "Size is correct");
        assertTrue(msg.toString().endsWith(OP_PING)); // toString COVERAGE
        assertEquals(0, msg.copyNotEmptyHeaders(0, new byte[0])); // coverage for copyNotEmptyHeaders which is a no-op
    }

    @Test
    public void testSizeOnPublishMessage() {
        byte[] body = new byte[10];
        String subject = "subj";
        String replyTo = "reply";
        String protocol = "PUB " + subject + " " + replyTo + " " + body.length;

        NatsMessage msg = new NatsMessage(subject, replyTo, body);

        assertEquals(msg.getProtocolBytes().length + body.length + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.US_ASCII).length + body.length + 4, msg.getSizeInBytes(), "Size is correct");

        msg = new NatsMessage(subject, replyTo, body);

        assertEquals(msg.getProtocolBytes().length + body.length + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.UTF_8).length + body.length + 4, msg.getSizeInBytes(), "Size is correct");
    }

    @Test
    public void testSizeOnPublishMessageWithHeaders() {
        Headers headers = new Headers().add("Content-Type", "text/plain");
        byte[] body = new byte[10];
        String subject = "subj";
        String replyTo = "reply";
        String protocol = "HPUB " + subject + " " + replyTo + " " + headers.serializedLength() + " " + (headers.serializedLength() + body.length);

        NatsMessage msg = new NatsMessage(subject, replyTo, headers, body);

        assertEquals(msg.getProtocolBytes().length + headers.serializedLength() + body.length + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.US_ASCII).length + headers.serializedLength() + body.length + 4, msg.getSizeInBytes(), "Size is correct");

        msg = new NatsMessage(subject, replyTo, headers, body);

        assertEquals(msg.getProtocolBytes().length + headers.serializedLength() + body.length + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.UTF_8).length + headers.serializedLength() + body.length + 4, msg.getSizeInBytes(), "Size is correct");
    }

    @Test
    public void testSizeOnPublishMessageOnlyHeaders() {
        Headers headers = new Headers().add("Content-Type", "text/plain");
        String subject = "subj";
        String replyTo = "reply";
        String protocol = "HPUB " + subject + " " + replyTo + " " + headers.serializedLength() + " " + headers.serializedLength();

        NatsMessage msg = new NatsMessage(subject, replyTo, headers, null);

        assertEquals(msg.getProtocolBytes().length + headers.serializedLength() + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.US_ASCII).length + headers.serializedLength() + 4, msg.getSizeInBytes(), "Size is correct");

        msg = new NatsMessage(subject, replyTo, headers, null);

        assertEquals(msg.getProtocolBytes().length + headers.serializedLength() + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.UTF_8).length + headers.serializedLength() + 4, msg.getSizeInBytes(), "Size is correct");
    }

    @Test
    public void testSizeOnPublishMessageOnlySubject() {
        String subject = "subj";
        String replyTo = "reply";
        String protocol = "PUB " + subject + " " + replyTo + " " + 0;

        NatsMessage msg = new NatsMessage(subject, replyTo, null, null);

        assertEquals(msg.getProtocolBytes().length + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.US_ASCII).length + 4, msg.getSizeInBytes(), "Size is correct");

        msg = new NatsMessage(subject, replyTo, null, null);

        assertEquals(msg.getProtocolBytes().length + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.UTF_8).length + 4, msg.getSizeInBytes(), "Size is correct");
    }

    @Test
    public void testCustomMaxControlLine() throws Exception {
        byte[] body = new byte[10];
        int maxControlLine = 1024;

        StringBuilder subject = new StringBuilder(random());
        while (subject.length() <= maxControlLine) {
            subject.append(subject);
        }

        runInSharedOwnNc(optionsBuilder().maxReconnects(0).maxControlLine(maxControlLine),
            nc -> assertThrows(IllegalArgumentException.class, () -> nc.request(subject.toString(), body)));
    }

    @Test
    public void testBigProtocolLine() throws Exception {
        StringBuilder subject = new StringBuilder(random());
        while (subject.length() <= Options.DEFAULT_MAX_CONTROL_LINE) {
            subject.append(subject);
        }
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(ExitAt.NO_EXIT)) {
            try (Connection nc = ConnectionUtils.standardConnect(options(mockTs))) {
                // Without Body
                assertThrows(IllegalArgumentException.class, () -> nc.subscribe(subject.toString()));

                // With Body
                byte[] body = new byte[10];
                String replyTo = "reply";
                assertThrows(IllegalArgumentException.class, () -> nc.publish(subject.toString(), replyTo, body));
            }
        }
    }

    @Test
    public void notJetStream() throws Exception {
        NatsMessage m = testMessage();
        m.ack();
        m.ackSync(Duration.ZERO);
        m.nak();
        m.nakWithDelay(Duration.ZERO);
        m.nakWithDelay(0);
        m.inProgress();
        m.term();
        assertThrows(IllegalStateException.class, m::metaData);
    }

    @Test
    public void miscCoverage() {
        //noinspection deprecation
        NatsMessage m = NatsMessage.builder()
                .subject("test").replyTo("reply").utf8mode(true)
                .data("data", StandardCharsets.US_ASCII)
                .build();
        assertFalse(m.hasHeaders());
        assertFalse(m.isJetStream());
        assertFalse(m.isStatusMessage());
        assertNotNull(m.toString());
        assertNotNull(m.toDetailString());
        assertFalse(m.isProtocol());
        assertFalse(m.isProtocolFilterOnStop());

        m = NatsMessage.builder()
            .subject("test").replyTo("reply")
            .data("very long data to string truncates with dot dot dot", StandardCharsets.US_ASCII)
            .build();
        assertNotNull(m.toString());
        assertTrue(m.toString().contains("..."));

        // no reply to, no data
        m = NatsMessage.builder().subject("test").build();
        assertNotNull(m.toString());
        assertNotNull(m.toDetailString());

        // no reply to, no empty data
        m = NatsMessage.builder().subject("test").data(new byte[0]).build();
        assertNotNull(m.toString());
        assertNotNull(m.toDetailString());

        // no reply to, no empty data
        m = NatsMessage.builder().subject("test").data((byte[])null).build();
        assertNotNull(m.toString());
        assertNotNull(m.toDetailString());

        // no reply to, no empty data
        m = NatsMessage.builder().subject("test").data((String)null).build();
        assertNotNull(m.toString());
        assertNotNull(m.toDetailString());

        List<String> data = dataAsLines("utf8-test-strings.txt");
        for (String d : data) {
            Message m1 = NatsMessage.builder().subject("test").data(d).build();
            Message m2 = NatsMessage.builder().subject("test").data(d, StandardCharsets.UTF_8).build();
            assertByteArraysEqual(m1.getData(), m2.getData());
        }

        m = testMessage();
        assertTrue(m.hasHeaders());
        assertNotNull(m.getHeaders());
        assertFalse(m.isUtf8mode()); // coverage, ALWAYS FALSE SINCE DISUSED
        assertFalse(m.getHeaders().isEmpty());
        assertNull(m.getSubscription());
        assertNull(m.getNatsSubscription());
        assertNull(m.getConnection());
        assertEquals(23, m.getControlLineLength());
        assertNotNull(m.toDetailString()); // COVERAGE

        m.headers = null; // we can do this because we have package access
        assertFalse(m.hasHeaders());
        assertNull(m.getHeaders());
        assertNotNull(m.toString()); // COVERAGE

        ProtocolMessage pmFilterOnStop = new ProtocolMessage(new byte[0]);
        ProtocolMessage pmNotFilterOnStop = new ProtocolMessage(pmFilterOnStop.getProtocolBab(), false);

        validateProto(pmFilterOnStop, true);
        validateProto(pmNotFilterOnStop, false);

        // retains filter on stop
        validateProto(new ProtocolMessage(pmFilterOnStop), true);
        validateProto(new ProtocolMessage(pmNotFilterOnStop), false);

        // sets filter on stop
        validateProto(new ProtocolMessage(pmFilterOnStop.getProtocolBab(), true), true);
        validateProto(new ProtocolMessage(pmFilterOnStop.getProtocolBab(), false), false);
        validateProto(new ProtocolMessage(pmNotFilterOnStop.getProtocolBab(), true), true);
        validateProto(new ProtocolMessage(pmNotFilterOnStop.getProtocolBab(), false), false);

        IncomingMessage scm = new IncomingMessage() {};
        assertEquals(0, scm.getSizeInBytes());
        assertThrows(IllegalStateException.class, scm::getProtocolBab);
        assertThrows(IllegalStateException.class, scm::getProtocolBytes);
        assertThrows(IllegalStateException.class, scm::getControlLineLength);
        assertFalse(scm.isProtocol());
        assertFalse(scm.isProtocolFilterOnStop());

        // coverage coverage coverage
        //noinspection deprecation
        NatsMessage nmCov = new NatsMessage("sub", "reply", null, true);
        nmCov.calculate();

        assertTrue(nmCov.toDetailString().contains("PUB sub reply 0"));
    }

    private static void validateProto(ProtocolMessage pm, boolean isProtocolFilterOnStop) {
        assertNotNull(pm.getProtocolBab());
        assertEquals(0, pm.getProtocolBab().length());
        assertEquals(2, pm.getSizeInBytes());
        assertEquals(2, pm.getControlLineLength());
        assertTrue(pm.isProtocol());
        assertEquals(isProtocolFilterOnStop, pm.isProtocolFilterOnStop());
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

    @Test
    public void testFactoryProducesStatusMessage() {
        IncomingHeadersProcessor incomingHeadersProcessor =
                new IncomingHeadersProcessor("NATS/1.0 503 No Responders\r\n".getBytes());
        IncomingMessageFactory factory =
                new IncomingMessageFactory("sid", "subj", "replyTo", 0, false);
        factory.setHeaders(incomingHeadersProcessor);
        factory.setData(null); // coverage

        Message m = factory.getMessage();
        assertTrue(m.isStatusMessage());
        assertNotNull(m.getStatus());
        assertEquals(503, m.getStatus().getCode());
        assertNotNull(m.getStatus().toString());
        StatusMessage sm = (StatusMessage)m;
        assertNotNull(sm.toString());
    }

    private NatsMessage testMessage() {
        Headers h = new Headers();
        h.add("key", "value");

        return NatsMessage.builder()
                .subject("test").replyTo("reply").headers(h)
                .data("data", StandardCharsets.US_ASCII)
                .build();
    }

    @Test
    public void testHeadersMutableBeforePublish() throws Exception {
        runInShared(nc -> {
            String subject = random();
            Subscription sub = nc.subscribe(subject);

            Headers h = new Headers();
            h.put("one", "A");
            Message m = new NatsMessage(subject, null, h, null);
            nc.publish(m);
            Message incoming = sub.nextMessage(1000);
            assertEquals(1, incoming.getHeaders().size());

            // headers are no longer copied, just referenced
            // so this will affect the message which is the same
            // as the local copy
            h.put("two", "B");
            nc.publish(m);
            incoming = sub.nextMessage(1000);
            assertEquals(2, incoming.getHeaders().size());

            // also if you get the headers from the message
            m.getHeaders().put("three", "C");
            nc.publish(m);
            incoming = sub.nextMessage(1000);
            assertEquals(3, incoming.getHeaders().size());
        });
    }
}