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

import io.nats.client.support.DateTimeUtils;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class ConsumerConfigurationTests extends TestBase {

    @Test
    public void testBuilder() {
        ZonedDateTime zdt = ZonedDateTime.of(2012, 1, 12, 6, 30, 1, 500, DateTimeUtils.ZONE_ID_GMT);

        ConsumerConfiguration c = ConsumerConfiguration.builder()
                .ackPolicy(AckPolicy.Explicit)
                .ackWait(Duration.ofSeconds(99)) // duration
                .deliverPolicy(DeliverPolicy.ByStartSequence)
                .durable(DURABLE)
                .filterSubject("fs")
                .maxDeliver(5555)
                .maxAckPending(6666)
                .rateLimit(4242)
                .replayPolicy(ReplayPolicy.Original)
                .sampleFrequency("10s")
                .startSequence(2001)
                .startTime(zdt)
                .deliverSubject(DELIVER)
                .idleHeartbeat(Duration.ofSeconds(66)) // duration
                .flowControl(true)
                .build();

        assertEquals(AckPolicy.Explicit, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(99), c.getAckWait());
        assertEquals(Duration.ofSeconds(66), c.getIdleHeartbeat());
        assertEquals(DeliverPolicy.ByStartSequence, c.getDeliverPolicy());
        assertEquals(DELIVER, c.getDeliverSubject());
        assertEquals(DURABLE, c.getDurable());
        assertEquals("fs", c.getFilterSubject());
        assertEquals(5555, c.getMaxDeliver());
        assertEquals(6666, c.getMaxAckPending());
        assertEquals(4242, c.getRateLimit());
        assertEquals(ReplayPolicy.Original, c.getReplayPolicy());
        assertEquals(2001, c.getStartSequence());
        assertEquals(zdt, c.getStartTime());
        assertTrue(c.getFlowControl());

        ConsumerCreateRequest ccr = new ConsumerCreateRequest(STREAM, c);
        assertEquals(STREAM, ccr.getStreamName());
        assertNotNull(ccr.getConfig());

        String json = ccr.toJson();
        c = new ConsumerConfiguration(json);
        assertEquals(AckPolicy.Explicit, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(99), c.getAckWait());
        assertEquals(Duration.ofSeconds(66), c.getIdleHeartbeat());
        assertEquals(DeliverPolicy.ByStartSequence, c.getDeliverPolicy());
        assertEquals(DELIVER, c.getDeliverSubject());
        assertEquals(DURABLE, c.getDurable());
        assertEquals("fs", c.getFilterSubject());
        assertEquals(5555, c.getMaxDeliver());
        assertEquals(4242, c.getRateLimit());
        assertEquals(ReplayPolicy.Original, c.getReplayPolicy());
        assertEquals(2001, c.getStartSequence());
        assertEquals(zdt.toEpochSecond(), c.getStartTime().toEpochSecond());
        assertTrue(c.getFlowControl());

        assertNotNull(ccr.toString()); // COVERAGE
        assertNotNull(c.toString()); // COVERAGE

        // millis instead of duration coverage
        // supply null as deliverPolicy, ackPolicy , replayPolicy,
        c = ConsumerConfiguration.builder()
                .deliverPolicy(null)
                .ackPolicy(null)
                .replayPolicy(null)
                .ackWait(9000) // millis
                .idleHeartbeat(6000) // millis
                .build();

        assertEquals(AckPolicy.Explicit, c.getAckPolicy());
        assertEquals(DeliverPolicy.All, c.getDeliverPolicy());
        assertEquals(ReplayPolicy.Instant, c.getReplayPolicy());
        assertEquals(Duration.ofSeconds(9), c.getAckWait());
        assertEquals(Duration.ofSeconds(6), c.getIdleHeartbeat());
    }

    @Test
    public void testParsingAndSetters() {
        String configJSON = dataAsString("ConsumerConfiguration.json");
        ConsumerConfiguration c = new ConsumerConfiguration(configJSON);
        assertEquals(DeliverPolicy.All, c.getDeliverPolicy());
        assertEquals(AckPolicy.All, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(30), c.getAckWait());
        assertEquals(Duration.ofSeconds(20), c.getIdleHeartbeat());
        assertEquals(10, c.getMaxDeliver());
        assertEquals(73, c.getRateLimit());
        assertEquals(ReplayPolicy.Original, c.getReplayPolicy());
        assertEquals(2020, c.getStartTime().getYear(), 2020);
        assertEquals(21, c.getStartTime().getSecond(), 21);
        assertEquals("foo-durable", c.getDurable());
        assertEquals("bar", c.getDeliverSubject());
        assertEquals("foo-filter", c.getFilterSubject());
        assertEquals(42, c.getMaxAckPending());
        assertEquals("sample_freq-value", c.getSampleFrequency());
        assertTrue(c.getFlowControl());
    }
}
