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
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PublishOptionsTests {
    @Test
    public void testDefaultOptions() {
        PublishOptions o = new PublishOptions.Builder().build();

        assertEquals(PublishOptions.UNSET_STREAM, o.getStream(), "default stream");
        assertEquals(PublishOptions.DEFAULT_TIMEOUT, o.getStreamTimeout(), "default timeout");
    }

    @Test
    public void testChainedOptions() {
        PublishOptions o = new PublishOptions.Builder().
            stream("foo").
            streamTimeout(Duration.ofSeconds(99)).
            expectedLastMsgId("1").
            expectedStream("bar").
            expectedLastSeqence(42).
            build();

        assertEquals("foo", o.getStream(), "stream");
        assertEquals(Duration.ofSeconds(99), o.getStreamTimeout(), "timeout");
        assertEquals("1", o.getExpectedLastMsgId(), "expected msgid");
        assertEquals(42, o.getExpectedLastSequence(), "expected last seqno");
        assertEquals("bar", o.getExpectedStream(), "expected stream");
    }

    @Test
    public void testProperties() {
        Properties p = new Properties();
        p.setProperty(PublishOptions.PROP_PUBLISH_TIMEOUT, "PT20M");
        p.setProperty(PublishOptions.PROP_STREAM_NAME, "foo");
        PublishOptions o = new PublishOptions.Builder(p).build();
        assertEquals("foo", o.getStream(), "stream foo");
        assertEquals(Duration.ofMinutes(20), o.getStreamTimeout(), "20M timeout");

    }
}