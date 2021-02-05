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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PublishOptionsTests extends TestBase {
    @Test
    public void testDefaultOptions() {
        PublishOptions po = new PublishOptions.Builder().build();
        assertEquals(PublishOptions.UNSET_STREAM, po.getStream(), "default stream");
        assertEquals(PublishOptions.DEFAULT_TIMEOUT, po.getStreamTimeout(), "default timeout");
        assertEquals(PublishOptions.UNSET_LAST_SEQUENCE, po.getExpectedLastSequence());
    }

    @Test
    public void testChainedOptions() {
        PublishOptions po = new PublishOptions.Builder()
                .stream(STREAM)
                .streamTimeout(Duration.ofSeconds(99))
                .expectedLastMsgId("1")
                .expectedStream("bar")
                .expectedLastSeqence(42)
                .build();

        assertEquals(STREAM, po.getStream(), "stream");
        assertEquals(Duration.ofSeconds(99), po.getStreamTimeout(), "timeout");
        assertEquals("1", po.getExpectedLastMsgId(), "expected msgid");
        assertEquals(42, po.getExpectedLastSequence(), "expected last seqno");
        assertEquals("bar", po.getExpectedStream(), "expected stream");
    }

    @Test
    public void testProperties() {
        Properties p = new Properties();
        p.setProperty(PublishOptions.PROP_PUBLISH_TIMEOUT, "PT20M");
        p.setProperty(PublishOptions.PROP_STREAM_NAME, STREAM);
        PublishOptions po = new PublishOptions.Builder(p).build();
        assertEquals(STREAM, po.getStream(), "stream foo");
        assertEquals(Duration.ofMinutes(20), po.getStreamTimeout(), "20M timeout");
    }
}