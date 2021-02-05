// Copyright 2020 The NATS Authors
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

package io.nats.client;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamOptionObjectsTests {

    @Test
    public void testJetStreamOptions() {
        JetStreamOptions jso = JetStreamOptions.defaultOptions();
        assertNull(jso.getPrefix());
        assertEquals(Options.DEFAULT_CONNECTION_TIMEOUT, jso.getRequestTimeout());

        jso = JetStreamOptions.builder()
                .prefix("pre")
                .requestTimeout(Duration.ofSeconds(42))
                .build();
        assertEquals("pre", jso.getPrefix());
        assertEquals(Duration.ofSeconds(42), jso.getRequestTimeout());
    }

    @Test
    public void testJetStreamOptionsInvalidPrefix() {
        assertThrows(IllegalArgumentException.class, () -> {
            JetStreamOptions.builder().prefix(">").build();
        });
        assertThrows(IllegalArgumentException.class, () -> {
            JetStreamOptions.builder().prefix("*").build();
        });
    }

    @Test
    public void testSubscribeOptions() {
        PushSubscribeOptions so = PushSubscribeOptions.builder()
                .stream("strm")
                .durable("drbl")
                .deliverSubject("dlvrsbjct")
                .build();

        assertEquals("strm", so.getStream());
        assertEquals("drbl", so.getDurable());
        assertEquals("dlvrsbjct", so.getDeliverSubject());
    }

    @Test
    public void testPublishOptions() {
        PublishOptions po = PublishOptions.builder()
                .stream("strm")
                .streamTimeout(Duration.ofSeconds(42))
                .expectedStream("expectedStream")
                .expectedLastMsgId("expectedLastMsgId")
                .expectedLastSeqence(73)
                .messageId("messageId")
                .build();

        assertEquals("strm", po.getStream());
        assertEquals(Duration.ofSeconds(42), po.getStreamTimeout());
        assertEquals("expectedStream", po.getExpectedStream());
        assertEquals("expectedLastMsgId", po.getExpectedLastMsgId());
        assertEquals(73, po.getExpectedLastSequence());
        assertEquals("messageId", po.getMessageId());

        po = PublishOptions.builder().build();
        assertEquals(PublishOptions.UNSET_STREAM, po.getStream());
        assertEquals(PublishOptions.DEFAULT_TIMEOUT, po.getStreamTimeout());
        assertEquals(PublishOptions.UNSET_LAST_SEQUENCE, po.getExpectedLastSequence());

    }
}