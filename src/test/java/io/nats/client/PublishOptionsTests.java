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

import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class PublishOptionsTests extends TestBase {

    @Test
    public void testBuilder() {
        PublishOptions.Builder builder = PublishOptions.builder();
        PublishOptions po = builder.build();
        //noinspection deprecation
        assertNull(po.getStream());
        assertEquals(PublishOptions.DEFAULT_TIMEOUT, po.getStreamTimeout());
        assertNull(po.getExpectedStream());
        assertNull(po.getExpectedLastMsgId());
        assertEquals(PublishOptions.UNSET_LAST_SEQUENCE, po.getExpectedLastSequence());
        assertEquals(PublishOptions.UNSET_LAST_SEQUENCE, po.getExpectedLastSubjectSequence());
        assertNull(po.getExpectedLastSubjectSequenceSubject());
        assertNull(po.getMessageTtl());

        Duration streamTimeout = Duration.ofSeconds(99);
        //noinspection deprecation
        builder.stream("pubAckStream"); // DEPRECATED SO JUST COVERAGE

        po = builder
            .streamTimeout(streamTimeout)
            .expectedStream("expectedStream")
            .expectedLastMsgId("1")
            .expectedLastSequence(42)
            .expectedLastSubjectSequence(43)
            .expectedLastSubjectSequenceSubject("sss")
            .messageId("msgId")
            .messageTtlCustom("custom")
            .build();

        //noinspection deprecation
        assertEquals("pubAckStream", po.getStream()); // DEPRECATED / COVERAGE
        assertEquals(streamTimeout, po.getStreamTimeout());
        assertEquals("expectedStream", po.getExpectedStream());
        assertEquals("1", po.getExpectedLastMsgId());
        assertEquals(42, po.getExpectedLastSequence());
        assertEquals(43, po.getExpectedLastSubjectSequence());
        assertEquals("sss", po.getExpectedLastSubjectSequenceSubject());
        assertEquals("msgId", po.getMessageId());
        assertEquals("custom", po.getMessageTtl());

        // test clearExpected
        po = builder.clearExpected().build();

        // these are not cleared
        assertEquals("expectedStream", po.getExpectedStream());
        assertEquals(Duration.ofSeconds(99), po.getStreamTimeout());
        assertEquals("custom", po.getMessageTtl());

        // these are cleared
        assertNull(po.getExpectedLastMsgId());
        assertEquals(PublishOptions.UNSET_LAST_SEQUENCE, po.getExpectedLastSequence());
        assertEquals(PublishOptions.UNSET_LAST_SEQUENCE, po.getExpectedLastSubjectSequence());
        assertNull(po.getExpectedLastSubjectSequenceSubject());
        assertNull(po.getMessageId());

        //noinspection deprecation
        po = builder.stream(null).streamTimeout(null).build();
        //noinspection deprecation
        assertNull(po.getStream());
        assertEquals(PublishOptions.DEFAULT_TIMEOUT, po.getStreamTimeout());

        //noinspection deprecation
        po = builder.stream("pubAckStream").build();
        //noinspection deprecation
        assertEquals("pubAckStream", po.getStream());

        //noinspection deprecation
        po = builder.stream("").build();
        //noinspection deprecation
        assertNull(po.getStream());
    }

    @Test
    public void testProperties() {
        Properties p = new Properties();
        p.setProperty(PublishOptions.PROP_PUBLISH_TIMEOUT, "PT20M");
        p.setProperty(PublishOptions.PROP_STREAM_NAME, STREAM);
        PublishOptions po = new PublishOptions.Builder(p).build();
        //noinspection deprecation
        assertEquals(STREAM, po.getStream(), "stream foo");
        assertEquals(Duration.ofMinutes(20), po.getStreamTimeout(), "20M timeout");

        p = new Properties();
        po = new PublishOptions.Builder(p).build();
        //noinspection deprecation
        assertNull(po.getStream());
        assertEquals(PublishOptions.DEFAULT_TIMEOUT, po.getStreamTimeout());
    }

    @Test
    public void testMessageTtl() {
        PublishOptions po = PublishOptions.builder().messageTtlSeconds(3).build();
        assertEquals("3s", po.getMessageTtl());

        po = PublishOptions.builder().messageTtlCustom("abcd").build();
        assertEquals("abcd", po.getMessageTtl());

        po = PublishOptions.builder().messageTtlNever().build();
        assertEquals("never", po.getMessageTtl());

        po = PublishOptions.builder().messageTtl(MessageTtl.seconds(3)).build();
        assertEquals("3s", po.getMessageTtl());

        po = PublishOptions.builder().messageTtl(MessageTtl.custom("abcd")).build();
        assertEquals("abcd", po.getMessageTtl());

        po = PublishOptions.builder().messageTtl(MessageTtl.never()).build();
        assertEquals("never", po.getMessageTtl());

        po = PublishOptions.builder().messageTtlSeconds(0).build();
        assertNull(po.getMessageTtl());

        po = PublishOptions.builder().messageTtlSeconds(-1).build();
        assertNull(po.getMessageTtl());

        po = PublishOptions.builder().messageTtlCustom(null).build();
        assertNull(po.getMessageTtl());

        po = PublishOptions.builder().messageTtlCustom("").build();
        assertNull(po.getMessageTtl());

        po = PublishOptions.builder().messageTtl(null).build();
        assertNull(po.getMessageTtl());

        assertThrows(IllegalArgumentException.class, () -> MessageTtl.seconds(0));
        assertThrows(IllegalArgumentException.class, () -> MessageTtl.seconds(-1));
        assertThrows(IllegalArgumentException.class, () -> MessageTtl.custom(null));
        assertThrows(IllegalArgumentException.class, () -> MessageTtl.custom(""));

        assertTrue(MessageTtl.seconds(3).toString().contains("3s")); // COVERAGE
    }
}
