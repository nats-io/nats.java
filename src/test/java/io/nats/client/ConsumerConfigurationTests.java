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

import io.nats.client.ConsumerConfiguration.AckPolicy;
import io.nats.client.ConsumerConfiguration.DeliverPolicy;
import io.nats.client.ConsumerConfiguration.ReplayPolicy;
import io.nats.client.impl.JsonUtils;
import io.nats.client.utils.ResourceUtils;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerConfigurationTests extends TestBase {
    @Test
    public void testBuilder() {
        ZonedDateTime zdt = ZonedDateTime.of(2012, 1, 12, 6, 30, 1, 500, JsonUtils.ZONE_ID_GMT);

        ConsumerConfiguration c = ConsumerConfiguration.builder()
                .ackPolicy(AckPolicy.Explicit)
                .ackWait(Duration.ofSeconds(99))
                .deliverPolicy(DeliverPolicy.ByStartSequence)
                .durable(DURABLE)
                .filterSubject("fs")
                .maxDeliver(5555)
                .maxAckPending(6666)
                .maxWaiting(7777)
                .rateLimit(4242)
                .replayPolicy(ReplayPolicy.Original)
                .sampleFrequency("10s")
                .startSequence(2001)
                .startTime(zdt)
                .deliverSubject(DELIVER)
                .build();

        assertEquals(AckPolicy.Explicit, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(99), c.getAckWait());
        assertEquals(DeliverPolicy.ByStartSequence, c.getDeliverPolicy());
        assertEquals(DELIVER, c.getDeliverSubject());
        assertEquals(DURABLE, c.getDurable());
        assertEquals("fs", c.getFilterSubject());
        assertEquals(5555, c.getMaxDeliver());
        assertEquals(6666, c.getMaxAckPending());
        assertEquals(7777, c.getMaxWaiting());
        assertEquals(4242, c.getRateLimit());
        assertEquals(ReplayPolicy.Original, c.getReplayPolicy());
        assertEquals(2001, c.getStartSequence());
        assertEquals(zdt, c.getStartTime());

        String json = c.toJSON(SUBJECT);
        c = new ConsumerConfiguration(json);
        assertEquals(AckPolicy.Explicit, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(99), c.getAckWait());
        assertEquals(DeliverPolicy.ByStartSequence, c.getDeliverPolicy());
        assertEquals(DELIVER, c.getDeliverSubject());
        assertEquals(DURABLE, c.getDurable());
        assertEquals("fs", c.getFilterSubject());
        assertEquals(5555, c.getMaxDeliver());
        assertEquals(4242, c.getRateLimit());
        assertEquals(ReplayPolicy.Original, c.getReplayPolicy());
        assertEquals(2001, c.getStartSequence());
        assertEquals(zdt.toEpochSecond(), c.getStartTime().toEpochSecond());
    }

    @Test
    public void testJSONParsing() {
        String configJSON = ResourceUtils.dataAsString("ConsumerConfiguration.json");
        ConsumerConfiguration c = new ConsumerConfiguration(configJSON);
        assertEquals("foo-durable", c.getDurable());
        assertEquals("bar", c.getDeliverSubject());
        assertEquals(DeliverPolicy.All, c.getDeliverPolicy());
        assertEquals(AckPolicy.All, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(30), c.getAckWait());
        assertEquals(10, c.getMaxDeliver());
        assertEquals(ReplayPolicy.Original, c.getReplayPolicy());
        assertEquals(2020, c.getStartTime().getYear(), 2020);
        assertEquals(21, c.getStartTime().getSecond(), 21);
    }
}
