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
            .description("blah")
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
            .flowControl(66000) // duration
            .maxPullWaiting(73)
            .maxBatch(55)
            .maxExpires(77000) // duration
            .inactiveThreshold(88000) // duration
            .headersOnly(true)
            .backoff(1000, 2000, 3000)
            .build();

        assertAsBuilt(c, zdt);

        ConsumerCreateRequest ccr = new ConsumerCreateRequest(STREAM, c);
        assertEquals(STREAM, ccr.getStreamName());

        assertNotNull(ccr.getConfig());

        String json = ccr.toJson();
        c = new ConsumerConfiguration(json);
        assertAsBuilt(c, zdt);

        assertNotNull(ccr.toString()); // COVERAGE
        assertNotNull(c.toString()); // COVERAGE

        // flow control idle heartbeat combo
        c = ConsumerConfiguration.builder()
            .flowControl(Duration.ofMillis(501)).build();
        assertTrue(c.isFlowControl());
        assertEquals(501, c.getIdleHeartbeat().toMillis());

        c = ConsumerConfiguration.builder()
            .flowControl(502).build();
        assertTrue(c.isFlowControl());
        assertEquals(502, c.getIdleHeartbeat().toMillis());

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

        ConsumerConfiguration original = ConsumerConfiguration.builder().build();
        validateDefault(original);

        ConsumerConfiguration ccTest = ConsumerConfiguration.builder(null).build();
        validateDefault(ccTest);

        ccTest = new ConsumerConfiguration.Builder(null).build();
        validateDefault(ccTest);

        ccTest = ConsumerConfiguration.builder(original).build();
        validateDefault(ccTest);
    }

    private void validateDefault(ConsumerConfiguration cc) {
        assertDefaultCc(cc);
        assertFalse(cc.deliverPolicyWasSet());
        assertFalse(cc.ackPolicyWasSet());
        assertFalse(cc.replayPolicyWasSet());
        assertFalse(cc.startSeqWasSet());
        assertFalse(cc.maxDeliverWasSet());
        assertFalse(cc.rateLimitWasSet());
        assertFalse(cc.maxAckPendingWasSet());
        assertFalse(cc.maxPullWaitingWasSet());
        assertFalse(cc.flowControlWasSet());
        assertFalse(cc.headersOnlyWasSet());
        assertFalse(cc.maxBatchWasSet());
    }

    private void assertAsBuilt(ConsumerConfiguration c, ZonedDateTime zdt) {
        assertEquals(AckPolicy.Explicit, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(99), c.getAckWait());
        assertEquals(DeliverPolicy.ByStartSequence, c.getDeliverPolicy());
        assertEquals("blah", c.getDescription());
        assertEquals(DURABLE, c.getDurable());
        assertEquals("fs", c.getFilterSubject());
        assertEquals(5555, c.getMaxDeliver());
        assertEquals(6666, c.getMaxAckPending());
        assertEquals(4242, c.getRateLimit());
        assertEquals(ReplayPolicy.Original, c.getReplayPolicy());
        assertEquals("10s", c.getSampleFrequency());
        assertEquals(2001, c.getStartSequence());
        assertEquals(zdt, c.getStartTime());
        assertEquals(DELIVER, c.getDeliverSubject());
        assertTrue(c.isFlowControl());
        assertEquals(Duration.ofSeconds(66), c.getIdleHeartbeat());
        assertEquals(73, c.getMaxPullWaiting());
        assertEquals(55, c.getMaxBatch());
        assertEquals(Duration.ofSeconds(77), c.getMaxExpires());
        assertEquals(Duration.ofSeconds(88), c.getInactiveThreshold());
        assertTrue(c.isHeadersOnly());
        assertTrue(c.deliverPolicyWasSet());
        assertTrue(c.ackPolicyWasSet());
        assertTrue(c.replayPolicyWasSet());
        assertTrue(c.startSeqWasSet());
        assertTrue(c.maxDeliverWasSet());
        assertTrue(c.rateLimitWasSet());
        assertTrue(c.maxAckPendingWasSet());
        assertTrue(c.maxPullWaitingWasSet());
        assertTrue(c.flowControlWasSet());
        assertTrue(c.headersOnlyWasSet());
        assertTrue(c.maxBatchWasSet());
        assertEquals(3, c.getBackoff().size());
        assertEquals(Duration.ofSeconds(1), c.getBackoff().get(0));
        assertEquals(Duration.ofSeconds(2), c.getBackoff().get(1));
        assertEquals(Duration.ofSeconds(3), c.getBackoff().get(2));
    }

    @Test
    public void testParsingAndSetters() {
        String configJSON = dataAsString("ConsumerConfiguration.json");
        ConsumerConfiguration c = new ConsumerConfiguration(configJSON);
        assertEquals("foo-desc", c.getDescription());
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
        assertTrue(c.isFlowControl());
        assertEquals(128, c.getMaxPullWaiting());
        assertTrue(c.isHeadersOnly());
        assertEquals(99, c.getStartSequence());
        assertEquals(55, c.getMaxBatch());
        assertEquals(Duration.ofSeconds(40), c.getMaxExpires());
        assertEquals(Duration.ofSeconds(50), c.getInactiveThreshold());
        assertEquals(3, c.getBackoff().size());
        assertEquals(Duration.ofSeconds(1), c.getBackoff().get(0));
        assertEquals(Duration.ofSeconds(2), c.getBackoff().get(1));
        assertEquals(Duration.ofSeconds(3), c.getBackoff().get(2));

        assertDefaultCc(new ConsumerConfiguration("{}"));
    }

    private static void assertDefaultCc(ConsumerConfiguration c)
    {
        assertEquals(DeliverPolicy.All, c.getDeliverPolicy());
        assertEquals(AckPolicy.Explicit, c.getAckPolicy());
        assertEquals(ReplayPolicy.Instant, c.getReplayPolicy());
        assertNull(c.getDurable());
        assertNull(c.getDeliverGroup());
        assertNull(c.getDeliverSubject());
        assertNull(c.getFilterSubject());
        assertNull(c.getDescription());
        assertNull(c.getSampleFrequency());

        assertNull(c.getAckWait());
        assertNull(c.getIdleHeartbeat());

        assertNull(c.getStartTime());

        assertFalse(c.isFlowControl());
        assertFalse(c.isHeadersOnly());

        assertEquals(-1, c.getStartSequence());
        assertEquals(-1, c.getMaxDeliver());
        assertEquals(-1, c.getRateLimit());
        assertEquals(-1, c.getMaxAckPending());
        assertEquals(-1, c.getMaxPullWaiting());

        assertEquals(0, c.getBackoff().size());
    }
}
