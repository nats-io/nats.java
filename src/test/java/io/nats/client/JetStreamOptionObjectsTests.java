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

public class JetStreamOptionObjectsTests extends JetStreamTestBase {

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
        assertThrows(IllegalArgumentException.class, () -> JetStreamOptions.builder().prefix(">").build());
        assertThrows(IllegalArgumentException.class, () -> JetStreamOptions.builder().prefix("*").build());
    }

    @Test
    public void testPushSubscribeOptions() {
        PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder();

        PushSubscribeOptions so = builder.build();

        // starts out all null which is fine
        assertNull(so.getStream());
        assertNull(so.getDurable());
        assertNull(so.getDeliverSubject());

        so = builder.stream(STREAM).durable(DURABLE).deliverSubject(DELIVER).build();

        assertEquals(STREAM, so.getStream());
        assertEquals(DURABLE, so.getDurable());
        assertEquals(DELIVER, so.getDeliverSubject());

        // demonstrate that you can clear the builder
        so = builder.stream(null).deliverSubject(null).durable(null).build();
        assertNull(so.getStream());
        assertNull(so.getDurable());
        assertNull(so.getDeliverSubject());

        PushSubscribeOptions.Builder fieldError = PushSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, () -> fieldError.stream("foo."));
        assertThrows(IllegalArgumentException.class, () -> fieldError.durable("foo."));
    }

    @Test
    public void testPullSubscribeOptions() {
        PullSubscribeOptions so = PullSubscribeOptions.builder()
                .defaultBatchSize(1)
                .defaultNoWait(true)
                .stream(STREAM)
                .durable(DURABLE)
                .build();

        assertEquals(1, so.getDefaultBatchSize());
        assertTrue(so.getDefaultNoWait());
        assertEquals(STREAM, so.getStream());
        assertEquals(DURABLE, so.getDurable());

        PullSubscribeOptions.Builder buildError = PullSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, buildError::build);
        buildError.defaultBatchSize(1); // need batch size
        assertThrows(IllegalArgumentException.class, buildError::build);
        buildError.durable(DURABLE); // also need durable
        buildError.build();

        PullSubscribeOptions.Builder fieldError = PullSubscribeOptions.builder();
        assertThrows(IllegalArgumentException.class, () -> fieldError.defaultBatchSize(0));
        assertThrows(IllegalArgumentException.class, () -> fieldError.defaultBatchSize(-1));
        assertThrows(IllegalArgumentException.class, () -> fieldError.defaultBatchSize(257));
        assertThrows(IllegalArgumentException.class, () -> fieldError.stream("foo."));
        assertThrows(IllegalArgumentException.class, () -> fieldError.durable(null));
        assertThrows(IllegalArgumentException.class, () -> fieldError.durable("foo."));
    }

    @Test
    public void testPublishOptions() {
        PublishOptions po = PublishOptions.builder()
                .stream(STREAM)
                .streamTimeout(Duration.ofSeconds(42))
                .expectedStream("expectedStream")
                .expectedLastMsgId("expectedLastMsgId")
                .expectedLastSeqence(73)
                .messageId("messageId")
                .build();

        assertEquals(STREAM, po.getStream());
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