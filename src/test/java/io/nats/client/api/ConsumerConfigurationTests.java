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

import io.nats.client.api.ConsumerConfiguration.CcNumeric;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;

import static io.nats.client.support.NatsConstants.EMPTY;
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
            .headersOnly(true)
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
    }

    @Test
    public void testChanges() {
        ConsumerConfiguration original = ConsumerConfiguration.builder().build();
        assertFalse(original.wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).deliverPolicy(DeliverPolicy.All).build()
            .wouldBeChangeTo(original));
        assertTrue(ConsumerConfiguration.builder(original).deliverPolicy(DeliverPolicy.New).build()
            .wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).ackPolicy(AckPolicy.Explicit).build()
            .wouldBeChangeTo(original));
        assertTrue(ConsumerConfiguration.builder(original).ackPolicy(AckPolicy.None).build()
            .wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).replayPolicy(ReplayPolicy.Instant).build()
            .wouldBeChangeTo(original));
        assertTrue(ConsumerConfiguration.builder(original).replayPolicy(ReplayPolicy.Original).build()
            .wouldBeChangeTo(original));

        ConsumerConfiguration ccTest = ConsumerConfiguration.builder(original).flowControl(1000).build();
        assertFalse(ccTest.wouldBeChangeTo(ccTest));
        assertTrue(ccTest.wouldBeChangeTo(original));

        ccTest = ConsumerConfiguration.builder(original).idleHeartbeat(1000).build();
        assertFalse(ccTest.wouldBeChangeTo(ccTest));
        assertTrue(ccTest.wouldBeChangeTo(original));

        ccTest = ConsumerConfiguration.builder(original).startTime(ZonedDateTime.now()).build();
        assertFalse(ccTest.wouldBeChangeTo(ccTest));
        assertTrue(ccTest.wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).headersOnly(false).build()
            .wouldBeChangeTo(original));
        assertTrue(ConsumerConfiguration.builder(original).headersOnly(true).build()
            .wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).startSequence(CcNumeric.START_SEQ.initial).build()
            .wouldBeChangeTo(original));
        assertTrue(ConsumerConfiguration.builder(original).startSequence(new Long(99)).build()
            .wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).maxDeliver(CcNumeric.MAX_DELIVER.initial).build()
            .wouldBeChangeTo(original));
        assertTrue(ConsumerConfiguration.builder(original).maxDeliver(new Long(99)).build()
            .wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).rateLimit(CcNumeric.RATE_LIMIT.initial).build()
            .wouldBeChangeTo(original));
        assertTrue(ConsumerConfiguration.builder(original).rateLimit(new Long(99)).build()
            .wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).maxAckPending(CcNumeric.MAX_ACK_PENDING.initial).build()
            .wouldBeChangeTo(original));
        assertTrue(ConsumerConfiguration.builder(original).maxAckPending(new Long(99)).build()
            .wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).maxPullWaiting(CcNumeric.MAX_PULL_WAITING.initial).build()
            .wouldBeChangeTo(original));
        assertTrue(ConsumerConfiguration.builder(original).maxPullWaiting(new Long(99)).build()
            .wouldBeChangeTo(original));


        assertFalse(ConsumerConfiguration.builder(original).filterSubject(EMPTY).build()
            .wouldBeChangeTo(original));
        ccTest = ConsumerConfiguration.builder(original).filterSubject(PLAIN).build();
        assertFalse(ccTest.wouldBeChangeTo(ccTest));
        assertTrue(ccTest.wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).description(EMPTY).build()
            .wouldBeChangeTo(original));
        ccTest = ConsumerConfiguration.builder(original).description(PLAIN).build();
        assertFalse(ccTest.wouldBeChangeTo(ccTest));
        assertTrue(ccTest.wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).sampleFrequency(EMPTY).build()
            .wouldBeChangeTo(original));
        ccTest = ConsumerConfiguration.builder(original).sampleFrequency(PLAIN).build();
        assertFalse(ccTest.wouldBeChangeTo(ccTest));
        assertTrue(ccTest.wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).deliverSubject(EMPTY).build()
            .wouldBeChangeTo(original));
        ccTest = ConsumerConfiguration.builder(original).deliverSubject(PLAIN).build();
        assertFalse(ccTest.wouldBeChangeTo(ccTest));
        assertTrue(ccTest.wouldBeChangeTo(original));

        assertFalse(ConsumerConfiguration.builder(original).deliverGroup(EMPTY).build()
            .wouldBeChangeTo(original));
        ccTest = ConsumerConfiguration.builder(original).deliverGroup(PLAIN).build();
        assertFalse(ccTest.wouldBeChangeTo(ccTest));
        assertTrue(ccTest.wouldBeChangeTo(original));
    }

    private void assertAsBuilt(ConsumerConfiguration c, ZonedDateTime zdt) {
        assertEquals(AckPolicy.Explicit, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(99), c.getAckWait());
        assertEquals(Duration.ofSeconds(66), c.getIdleHeartbeat());
        assertEquals(DeliverPolicy.ByStartSequence, c.getDeliverPolicy());
        assertEquals(DELIVER, c.getDeliverSubject());
        assertEquals("blah", c.getDescription());
        assertEquals(DURABLE, c.getDurable());
        assertEquals("fs", c.getFilterSubject());
        assertEquals(5555, c.getMaxDeliver());
        assertEquals(6666, c.getMaxAckPending());
        assertEquals(4242, c.getRateLimit());
        assertEquals(ReplayPolicy.Original, c.getReplayPolicy());
        assertEquals(2001, c.getStartSequence());
        assertEquals(zdt, c.getStartTime());
        assertEquals(73, c.getMaxPullWaiting());
        assertTrue(c.isFlowControl());
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

        assertEquals(CcNumeric.START_SEQ.initial(), c.getStartSequence());
        assertEquals(CcNumeric.MAX_DELIVER.initial(), c.getMaxDeliver());
        assertEquals(CcNumeric.RATE_LIMIT.initial(), c.getRateLimit());
        assertEquals(CcNumeric.MAX_ACK_PENDING.initial(), c.getMaxAckPending());
        assertEquals(CcNumeric.MAX_PULL_WAITING.initial(), c.getMaxPullWaiting());
    }
}
