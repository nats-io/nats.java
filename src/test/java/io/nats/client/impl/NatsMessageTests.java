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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.NatsServerProtocolMock;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.NatsServerProtocolMock.ExitAt;

public class NatsMessageTests {
    @Test
    public void testSizeOnProtocolMessage() {
        NatsMessage msg = new NatsMessage.Builder().protocol("PING").build();

        assertEquals(msg.getProtocolBytes().length + 2, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals("PING".getBytes(StandardCharsets.UTF_8).length + 2, msg.getSizeInBytes(), "Size is correct");
    }

    @Test
    public void testSizeOnPublishMessage() {
        byte[] body = new byte[10];
        String subject = "subj";
        String replyTo = "reply";
        String protocol = "PUB " + subject + " " + replyTo + " " + body.length;

        NatsMessage msg = new NatsMessage.Builder().subject(subject).replyTo(replyTo).data(body).build();

        assertEquals(msg.getProtocolBytes().length + body.length + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.US_ASCII).length + body.length + 4, msg.getSizeInBytes(), "Size is correct");

        msg = new NatsMessage.Builder().subject(subject).replyTo(replyTo).data(body).utf8mode(true).build();

        assertEquals(msg.getProtocolBytes().length + body.length + 4, msg.getSizeInBytes(), "Size is set, with CRLF");
        assertEquals(protocol.getBytes(StandardCharsets.UTF_8).length + body.length + 4, msg.getSizeInBytes(), "Size is correct");
    }

    @Test
    public void testCustomMaxControlLine() {
        assertThrows(IllegalArgumentException.class, () -> {
            byte[] body = new byte[10];
            String subject = "subject";
            String replyTo = "reply";
            int maxControlLine = 1024;

            while (subject.length() <= maxControlLine) {
                subject = subject + subject;
            }

            try (NatsTestServer ts = new NatsTestServer()) {
                Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        maxControlLine(maxControlLine).
                        build();
                Connection nc = Nats.connect(options);
                try {
                    assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                    nc.publish(subject, replyTo, body);
                    assertFalse(true);
                } finally {
                    nc.close();
                    assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
                }
            }
        });
    }

    @Test
    public void testBigProtocolLineWithoutBody() {
        assertThrows(IllegalArgumentException.class, () -> {
            String subject = "subject";

            while (subject.length() <= Options.DEFAULT_MAX_CONTROL_LINE) {
                subject = subject + subject;
            }

            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT);
                 NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                nc.subscribe(subject);
                assertFalse(true);
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
                subject = subject + subject;
            }

            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT);
                 NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                nc.publish(subject, replyTo, body);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testKind() {
        // if you don't call either of the incoming setters sid / protocolLineLength and didn't call protocol
        // it will be REGULAR
        assertEquals(NatsMessage.Kind.REGULAR,
                new NatsMessage.Builder()
                        .subject("subject")
                        .replyTo("replyTo")
                        .data("data".getBytes())
                        .utf8mode(true)
                        .build()
                        .getKind());

        // if you call either of the incoming setters sid / protocolLineLength at any point
        // it will be INCOMING
        assertEquals(NatsMessage.Kind.INCOMING,
                new NatsMessage.Builder().sid("sid").build().getKind());

        assertEquals(NatsMessage.Kind.INCOMING,
                new NatsMessage.Builder().protocolLineLength(42).build().getKind());

        assertEquals(NatsMessage.Kind.INCOMING,
                new NatsMessage.Builder().subject("subject").sid("sid").build().getKind());

        assertEquals(NatsMessage.Kind.INCOMING,
                new NatsMessage.Builder().subject("subject").protocolLineLength(42).build().getKind());

        // if you call protocol it will be PROTOCOL
        assertEquals(NatsMessage.Kind.PROTOCOL,
                new NatsMessage.Builder().protocol("proto").build().getKind());

        assertEquals(NatsMessage.Kind.PROTOCOL,
                new NatsMessage.Builder().protocol("").build().getKind());
    }

    @Test
    public void testBuilderInvalidUses() {
        // you have to set something...
        assertThrows(IllegalStateException.class, () ->
                new NatsMessage.Builder().build());

        // once you do PROTOCOL, you can't call these
        assertThrows(IllegalStateException.class, () ->
                new NatsMessage.Builder().protocol("proto").subject("nope"));

        assertThrows(IllegalStateException.class, () ->
            new NatsMessage.Builder().protocol("proto").data(ByteBuffer.wrap("nope".getBytes())));

        assertThrows(IllegalStateException.class, () ->
            new NatsMessage.Builder().protocol("proto").data("nope".getBytes()));

        assertThrows(IllegalStateException.class, () ->
            new NatsMessage.Builder().protocol("proto").utf8mode(true));

        assertThrows(IllegalStateException.class, () ->
            new NatsMessage.Builder().protocol("proto").sid("nope"));

        assertThrows(IllegalStateException.class, () ->
            new NatsMessage.Builder().protocol("proto").protocolLineLength(42));

        // once you do REGULAR or INCOMING, you can't call the protocol
        assertThrows(IllegalStateException.class, () ->
                new NatsMessage.Builder().subject("subject").protocol("proto"));
    }
}