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

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class ConsumerInfoTests {

    @Test
    public void testConsumerInfo() {
        String json = dataAsString("ConsumerInfo.json");
        ConsumerInfo ci = new ConsumerInfo(json);
        assertEquals("foo-stream", ci.getStreamName());
        assertEquals("foo-consumer", ci.getName());

        assertEquals(1, ci.getDelivered().getConsumerSequence());
        assertEquals(2, ci.getDelivered().getStreamSequence());
        assertEquals(3, ci.getAckFloor().getConsumerSequence());
        assertEquals(4, ci.getAckFloor().getStreamSequence());

        assertEquals(24, ci.getNumPending());
        assertEquals(42, ci.getNumAckPending());
        assertEquals(42, ci.getRedelivered());

        ConsumerConfiguration c = ci.getConsumerConfiguration();
        assertEquals("foo-consumer", c.getDurable());
        assertEquals("bar", c.getDeliverSubject());
        assertEquals(ConsumerConfiguration.DeliverPolicy.All, c.getDeliverPolicy());
        assertEquals(ConsumerConfiguration.AckPolicy.All, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(30), c.getAckWait());
        assertEquals(10, c.getMaxDeliver());
        assertEquals(ConsumerConfiguration.ReplayPolicy.Original, c.getReplayPolicy());

        ci = new ConsumerInfo("{}");
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
