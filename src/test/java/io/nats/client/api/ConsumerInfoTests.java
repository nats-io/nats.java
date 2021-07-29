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

package io.nats.client.api;

import io.nats.client.support.Ulong;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.nats.client.support.JsonUtils.EMPTY_JSON;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class ConsumerInfoTests {

    @Test
    public void testConsumerInfo() {
        String json = dataAsString("ConsumerInfo.json");
        ConsumerInfo ci = new ConsumerInfo(json);
        assertEquals("foo-stream", ci.getStreamName());
        assertEquals("foo-consumer", ci.getName());

        assertEquals(new Ulong(1), ci.getDelivered().getConsumerSequenceNum());
        assertEquals(new Ulong(2), ci.getDelivered().getStreamSequenceNum());
        assertEquals(3, ci.getAckFloor().getConsumerSequence()); // coverage for deprecated
        assertEquals(4, ci.getAckFloor().getStreamSequence()); // coverage for deprecated

        assertEquals(24, ci.getNumPending());
        assertEquals(42, ci.getNumAckPending());
        assertEquals(42, ci.getRedelivered());

        ConsumerConfiguration c = ci.getConsumerConfiguration();
        assertEquals("foo-consumer", c.getDurable());
        assertEquals("bar", c.getDeliverSubject());
        assertEquals(DeliverPolicy.All, c.getDeliverPolicy());
        assertEquals(AckPolicy.All, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(30), c.getAckWait());
        assertEquals(10, c.getMaxDeliver());
        assertEquals(ReplayPolicy.Original, c.getReplayPolicy());

        ci = new ConsumerInfo(EMPTY_JSON);
        assertNull(ci.getStreamName());
        assertNull(ci.getName());
        assertNull(ci.getCreationTime());
        assertNotNull(ci.getConsumerConfiguration());
        assertNotNull(ci.getDelivered());
        assertNotNull(ci.getAckFloor());
        assertEquals(0, ci.getNumPending());
        assertEquals(0, ci.getNumWaiting());
        assertEquals(0, ci.getNumAckPending());
        assertEquals(0, ci.getRedelivered());
    }

    @Test
    public void testToString() {
        // COVERAGE
        String json = dataAsString("ConsumerInfo.json");
        assertNotNull(new ConsumerInfo(json).toString());
    }
}
