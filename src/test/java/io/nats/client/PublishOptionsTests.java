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

import io.nats.client.support.Ulong;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PublishOptionsTests extends TestBase {

    @Test
    public void testBuilder() {
        PublishOptions.Builder builder = PublishOptions.builder();
        PublishOptions po = builder.build();
        assertEquals(PublishOptions.UNSET_STREAM, po.getStream(), "default stream");
        assertEquals(PublishOptions.DEFAULT_TIMEOUT, po.getStreamTimeout(), "default timeout");
        assertEquals(PublishOptions.UNSET_LAST_SEQUENCE_NUM, po.getExpectedLastSequenceNum());

        po = builder
                .stream(STREAM)
                .streamTimeout(Duration.ofSeconds(99))
                .expectedLastMsgId("1")
                .expectedStream("bar")
                .expectedLastSequence(new Ulong(42))
                .messageId("msgId")
                .build();

        assertEquals(STREAM, po.getStream(), "stream");
        assertEquals(Duration.ofSeconds(99), po.getStreamTimeout(), "timeout");
        assertEquals("1", po.getExpectedLastMsgId(), "expected msgid");
        assertEquals(new Ulong(42), po.getExpectedLastSequenceNum(), "expected last seqno");
        assertEquals("bar", po.getExpectedStream(), "expected stream");
        assertEquals("msgId", po.getMessageId(), "expected message id");

        po = builder.clearExpected().build();
        assertNull(po.getExpectedLastMsgId(), "expected msgid");
        assertEquals(PublishOptions.UNSET_LAST_SEQUENCE_NUM, po.getExpectedLastSequenceNum(), "expected last seqno");
        assertEquals("bar", po.getExpectedStream(), "expected stream");
        assertNull(po.getMessageId(), "expected message id");

        po = builder.stream(null).streamTimeout(null).build();
        assertEquals(PublishOptions.UNSET_STREAM, po.getStream());
        assertEquals(PublishOptions.DEFAULT_TIMEOUT, po.getStreamTimeout());

        po = builder.stream(STREAM).build();
        assertEquals(STREAM, po.getStream());

        po = builder.stream("").build();
        assertEquals(PublishOptions.UNSET_STREAM, po.getStream());
    }

    @Test
    public void testProperties() {
        Properties p = new Properties();
        p.setProperty(PublishOptions.PROP_PUBLISH_TIMEOUT, "PT20M");
        p.setProperty(PublishOptions.PROP_STREAM_NAME, STREAM);
        PublishOptions po = new PublishOptions.Builder(p).build();
        assertEquals(STREAM, po.getStream(), "stream foo");
        assertEquals(Duration.ofMinutes(20), po.getStreamTimeout(), "20M timeout");

        p = new Properties();
        po = new PublishOptions.Builder(p).build();
        assertEquals(PublishOptions.UNSET_STREAM, po.getStream());
        assertEquals(PublishOptions.DEFAULT_TIMEOUT, po.getStreamTimeout());
    }
}
