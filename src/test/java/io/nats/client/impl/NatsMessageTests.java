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
import static org.junit.Assert.assertTrue;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.NatsServerProtocolMock;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.NatsServerProtocolMock.ExitAt;

public class NatsMessageTests {
    @Test
    public void testSizeOnProtocolMessage() {
        NatsMessage msg = new NatsMessage(CharBuffer.wrap("PING"));

        assertEquals("Size is set, with CRLF", msg.getProtocolBytes().length + 2, msg.getSizeInBytes());
        assertEquals("Size is correct", "PING".getBytes(StandardCharsets.UTF_8).length + 2, msg.getSizeInBytes());
    }
    
    @Test
    public void testSizeOnPublishMessage() {
        byte[] body = new byte[10];
        String subject = "subj";
        String replyTo = "reply";
        String protocol = "PUB "+subject+" "+replyTo+" "+body.length;

        NatsMessage msg = new NatsMessage(subject, replyTo, body, false);

        assertEquals("Size is set, with CRLF", msg.getProtocolBytes().length + body.length + 4, msg.getSizeInBytes());
        assertEquals("Size is correct", protocol.getBytes(StandardCharsets.US_ASCII).length + body.length + 4, msg.getSizeInBytes());
    
        msg = new NatsMessage(subject, replyTo, body, true);

        assertEquals("Size is set, with CRLF", msg.getProtocolBytes().length + body.length + 4, msg.getSizeInBytes());
        assertEquals("Size is correct", protocol.getBytes(StandardCharsets.UTF_8).length + body.length + 4, msg.getSizeInBytes());
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testCustomMaxControlLine() throws Exception {
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
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                nc.publish(subject, replyTo, body);
                assertFalse(true);
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testBigProtocolLineWithoutBody() throws Exception {
        String subject = "subject";

        while (subject.length() <= Options.DEFAULT_MAX_CONTROL_LINE) {
            subject = subject + subject;
        }

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT);
                    NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
            nc.subscribe(subject);
            assertFalse(true);
        }
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testBigProtocolLineWithBody() throws Exception {
        byte[] body = new byte[10];
        String subject = "subject";
        String replyTo = "reply";

        while (subject.length() <= Options.DEFAULT_MAX_CONTROL_LINE) {
            subject = subject + subject;
        }

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(ExitAt.NO_EXIT);
                    NatsConnection nc = (NatsConnection) Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
            nc.publish(subject, replyTo, body);
            assertFalse(true);
        }
    }
}