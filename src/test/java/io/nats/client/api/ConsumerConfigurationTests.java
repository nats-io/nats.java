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
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static io.nats.client.api.ConsumerConfiguration.*;
import static io.nats.client.support.NatsJetStreamClientError.JsConsumerNameDurableMismatch;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class ConsumerConfigurationTests extends TestBase {

    @Test
    public void testBuilder() {
        ZonedDateTime zdt = ZonedDateTime.of(2012, 1, 12, 6, 30, 1, 500, DateTimeUtils.ZONE_ID_GMT);
        Map<String, String> metadata = new HashMap<>();
        metadata.put("meta-foo", "meta-bar");

        ConsumerConfiguration c = ConsumerConfiguration.builder()
            .ackPolicy(AckPolicy.Explicit)
            .ackWait(Duration.ofSeconds(99)) // duration
            .deliverPolicy(DeliverPolicy.ByStartSequence)
            .description("blah")
            .name(NAME)
            .durable(NAME)
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
            .maxBytes(56)
            .maxExpires(77000) // duration
            .numReplicas(5)
            .pauseUntil(zdt)
            .inactiveThreshold(88000) // duration
            .headersOnly(true)
            .memStorage(true)
            .backoff(1000, 2000, 3000)
            .metadata(metadata)
            .build();

        assertAsBuilt(c, zdt);

        ConsumerCreateRequest ccr = new ConsumerCreateRequest(STREAM, c);
        assertEquals(STREAM, ccr.getStreamName());

        assertNotNull(ccr.getConfig());

        String json = ccr.getConfig().toJson();
        c = new ConsumerConfiguration(JsonParser.parseUnchecked(json));
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

        assertEquals(DEFAULT_ACK_POLICY, c.getAckPolicy());
        assertEquals(DEFAULT_DELIVER_POLICY, c.getDeliverPolicy());
        assertEquals(DEFAULT_REPLAY_POLICY, c.getReplayPolicy());
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

        // flow control coverage
        c = ConsumerConfiguration.builder().build();
        assertFalse(c.isFlowControl());

        c = ConsumerConfiguration.builder().flowControl(1000).build();
        assertTrue(c.isFlowControl());

        // headers only coverage
        c = ConsumerConfiguration.builder().build();
        assertFalse(c.isHeadersOnly());

        c = ConsumerConfiguration.builder().headersOnly(false).build();
        assertFalse(c.isHeadersOnly());

        c = ConsumerConfiguration.builder().headersOnly(true).build();
        assertTrue(c.isHeadersOnly());

        // mem storage coverage
        c = ConsumerConfiguration.builder().build();
        assertFalse(c.isMemStorage());

        c = ConsumerConfiguration.builder().memStorage(false).build();
        assertFalse(c.isMemStorage());

        c = ConsumerConfiguration.builder().memStorage(true).build();
        assertTrue(c.isMemStorage());

        // idleHeartbeat coverage
        c = ConsumerConfiguration.builder().idleHeartbeat(null).build();
        assertNull(c.getIdleHeartbeat());

        c = ConsumerConfiguration.builder().idleHeartbeat(Duration.ZERO).build();
        assertEquals(DURATION_UNSET, c.getIdleHeartbeat());

        c = ConsumerConfiguration.builder().idleHeartbeat(0).build();
        assertEquals(DURATION_UNSET, c.getIdleHeartbeat());

        c = ConsumerConfiguration.builder().idleHeartbeat(Duration.ofMillis(MIN_IDLE_HEARTBEAT_MILLIS + 1)).build();
        assertEquals(Duration.ofMillis(MIN_IDLE_HEARTBEAT_MILLIS + 1), c.getIdleHeartbeat());

        c = ConsumerConfiguration.builder().idleHeartbeat(MIN_IDLE_HEARTBEAT_MILLIS + 1).build();
        assertEquals(Duration.ofMillis(MIN_IDLE_HEARTBEAT_MILLIS + 1), c.getIdleHeartbeat());

        assertThrows(IllegalArgumentException.class,
            () -> ConsumerConfiguration.builder().idleHeartbeat(Duration.ofMillis(MIN_IDLE_HEARTBEAT_MILLIS - 1)).build());

        assertThrows(IllegalArgumentException.class,
            () -> ConsumerConfiguration.builder().idleHeartbeat(MIN_IDLE_HEARTBEAT_MILLIS - 1).build());

        // backoff coverage
        c = ConsumerConfiguration.builder().backoff(Duration.ofSeconds(1), null, Duration.ofSeconds(2)).build();
        assertEquals(2, c.getBackoff().size());
        assertEquals(Duration.ofSeconds(1), c.getBackoff().get(0));
        assertEquals(Duration.ofSeconds(2), c.getBackoff().get(1));

        assertThrows(IllegalArgumentException.class,
            () -> ConsumerConfiguration.builder().backoff(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class,
            () -> ConsumerConfiguration.builder().backoff(Duration.ofNanos(DURATION_MIN_LONG - 1)).build());

        c = ConsumerConfiguration.builder().backoff(1000, 2000).build();
        assertEquals(2, c.getBackoff().size());
        assertEquals(Duration.ofSeconds(1), c.getBackoff().get(0));
        assertEquals(Duration.ofSeconds(2), c.getBackoff().get(1));

        assertThrows(IllegalArgumentException.class,
            () -> ConsumerConfiguration.builder().backoff(0).build());
        assertThrows(IllegalArgumentException.class,
            () -> ConsumerConfiguration.builder().backoff(DURATION_MIN_LONG - 1).build());

        assertClientError(JsConsumerNameDurableMismatch, () -> ConsumerConfiguration.builder().name(NAME).durable(DURABLE).build());
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
        assertFalse(cc.maxBytesWasSet());
        assertFalse(cc.numReplicasWasSet());
        assertFalse(cc.memStorageWasSet());
    }

    private void assertAsBuilt(ConsumerConfiguration c, ZonedDateTime zdt) {
        assertEquals(AckPolicy.Explicit, c.getAckPolicy());
        assertEquals(Duration.ofSeconds(99), c.getAckWait());
        assertEquals(DeliverPolicy.ByStartSequence, c.getDeliverPolicy());
        assertEquals("blah", c.getDescription());
        assertEquals(NAME, c.getDurable());
        assertEquals(NAME, c.getName());
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
        assertEquals(56, c.getMaxBytes());
        assertEquals(Duration.ofSeconds(77), c.getMaxExpires());
        assertEquals(Duration.ofSeconds(88), c.getInactiveThreshold());
        assertEquals(5, c.getNumReplicas());
        assertEquals(zdt, c.getPauseUntil());
        assertTrue(c.isHeadersOnly());
        assertTrue(c.isMemStorage());
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
        assertTrue(c.maxBytesWasSet());
        assertTrue(c.numReplicasWasSet());
        assertTrue(c.memStorageWasSet());
        assertEquals(3, c.getBackoff().size());
        assertEquals(Duration.ofSeconds(1), c.getBackoff().get(0));
        assertEquals(Duration.ofSeconds(2), c.getBackoff().get(1));
        assertEquals(Duration.ofSeconds(3), c.getBackoff().get(2));
        assertEquals(1, c.getMetadata().size());
        assertEquals("meta-bar", c.getMetadata().get("meta-foo"));
    }

    @Test
    public void testParsingAndSetters() {
        String json = dataAsString("ConsumerConfiguration.json");
        ConsumerConfiguration c = new ConsumerConfiguration(JsonParser.parseUnchecked(json));
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
        assertEquals("foo-name", c.getName());
        assertEquals("foo-name", c.getDurable());
        assertEquals("bar", c.getDeliverSubject());
        assertEquals("foo-filter", c.getFilterSubject());
        assertEquals(42, c.getMaxAckPending());
        assertEquals("sample_freq-value", c.getSampleFrequency());
        assertTrue(c.isFlowControl());
        assertEquals(128, c.getMaxPullWaiting());
        assertTrue(c.isHeadersOnly());
        assertTrue(c.isMemStorage());
        assertEquals(99, c.getStartSequence());
        assertEquals(55, c.getMaxBatch());
        assertEquals(56, c.getMaxBytes());
        assertEquals(5, c.getNumReplicas());
        assertEquals(Duration.ofSeconds(40), c.getMaxExpires());
        assertEquals(Duration.ofSeconds(50), c.getInactiveThreshold());
        assertEquals(3, c.getBackoff().size());
        assertEquals(Duration.ofSeconds(1), c.getBackoff().get(0));
        assertEquals(Duration.ofSeconds(2), c.getBackoff().get(1));
        assertEquals(Duration.ofSeconds(3), c.getBackoff().get(2));
        assertEquals(1, c.getMetadata().size());
        assertEquals("meta-bar", c.getMetadata().get("meta-foo"));

        assertDefaultCc(new ConsumerConfiguration(JsonValue.EMPTY_MAP));
    }

    private static void assertDefaultCc(ConsumerConfiguration c) {
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
        assertFalse(c.isMemStorage());

        assertEquals(0, c.getStartSequence());
        assertEquals(-1, c.getMaxDeliver());
        assertEquals(0, c.getRateLimit());
        assertEquals(-1, c.getMaxAckPending());
        assertEquals(-1, c.getMaxPullWaiting());
        assertEquals(-1, c.getNumReplicas());

        assertEquals(0, c.getBackoff().size());
    }

    @SuppressWarnings("ObviousNullCheck")
    @Test
    public void testUtilityMethods() {
        assertEquals(1, ConsumerConfiguration.getOrUnset(1));
        assertEquals(INTEGER_UNSET, ConsumerConfiguration.getOrUnset(INTEGER_UNSET));
        assertEquals(INTEGER_UNSET, ConsumerConfiguration.getOrUnset((Integer) null));

        assertEquals(1L, ConsumerConfiguration.getOrUnsetUlong(1L));
        assertEquals(ULONG_UNSET, ConsumerConfiguration.getOrUnsetUlong(ULONG_UNSET));
        assertEquals(ULONG_UNSET, ConsumerConfiguration.getOrUnsetUlong(null));
        assertEquals(ULONG_UNSET, ConsumerConfiguration.getOrUnsetUlong(-1L));

        assertEquals(Duration.ZERO, ConsumerConfiguration.getOrUnset(Duration.ZERO));
        assertEquals(DURATION_UNSET, ConsumerConfiguration.getOrUnset(DURATION_UNSET));
        assertEquals(DURATION_UNSET, ConsumerConfiguration.getOrUnset((Duration) null));

        //noinspection ConstantConditions
        assertNull(ConsumerConfiguration.normalize(null, STANDARD_MIN));
        assertEquals(0, ConsumerConfiguration.normalize(0L, STANDARD_MIN));
        assertEquals(1, ConsumerConfiguration.normalize(1L, STANDARD_MIN));
        assertEquals(INTEGER_UNSET, ConsumerConfiguration.normalize(LONG_UNSET, STANDARD_MIN));
        assertEquals(INTEGER_UNSET, ConsumerConfiguration.normalize(Long.MIN_VALUE, STANDARD_MIN));
        assertEquals(Integer.MAX_VALUE, ConsumerConfiguration.normalize(Long.MAX_VALUE, STANDARD_MIN));

        //noinspection ConstantConditions
        assertNull(ConsumerConfiguration.normalizeUlong(null));
        assertEquals(0, ConsumerConfiguration.normalizeUlong(0L));
        assertEquals(1, ConsumerConfiguration.normalizeUlong(1L));
        assertEquals(ULONG_UNSET, ConsumerConfiguration.normalizeUlong(ULONG_UNSET));
        assertEquals(ULONG_UNSET, ConsumerConfiguration.normalizeUlong(-1L));

        //noinspection ConstantConditions
        assertNull(ConsumerConfiguration.normalize((Duration) null));
        assertEquals(Duration.ofNanos(1), ConsumerConfiguration.normalize(Duration.ofNanos(1)));
        assertEquals(DURATION_UNSET, ConsumerConfiguration.normalize(DURATION_UNSET));
        assertEquals(DURATION_UNSET, ConsumerConfiguration.normalize(Duration.ZERO));

        assertEquals(Duration.ofMillis(1), ConsumerConfiguration.normalizeDuration(1));
        assertEquals(DURATION_UNSET, ConsumerConfiguration.normalizeDuration(0));

        assertEquals(DEFAULT_DELIVER_POLICY, ConsumerConfiguration.GetOrDefault((DeliverPolicy) null));
        assertEquals(DeliverPolicy.Last, ConsumerConfiguration.GetOrDefault(DeliverPolicy.Last));

        assertEquals(DEFAULT_ACK_POLICY, ConsumerConfiguration.GetOrDefault((AckPolicy) null));
        assertEquals(AckPolicy.All, ConsumerConfiguration.GetOrDefault(AckPolicy.All));

        assertEquals(DEFAULT_REPLAY_POLICY, ConsumerConfiguration.GetOrDefault((ReplayPolicy) null));
        assertEquals(ReplayPolicy.Original, ConsumerConfiguration.GetOrDefault(ReplayPolicy.Original));
    }

    @Test
    public void testDowngradeFromLongToInt() {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
            .maxDeliver(Long.MAX_VALUE)
            .maxAckPending(Long.MAX_VALUE)
            .maxPullWaiting(Long.MAX_VALUE)
            .maxBatch(Long.MAX_VALUE)
            .maxBytes(Long.MAX_VALUE)
            .build();

        Long maxDeliver = cc.getMaxDeliver();
        Long maxAckPending = cc.getMaxAckPending();
        Long maxPullWaiting = cc.getMaxPullWaiting();
        Long maxBatch = cc.getMaxBatch();
        Long maxBytes = cc.getMaxBytes();

        assertEquals(Integer.MAX_VALUE, maxDeliver);
        assertEquals(Integer.MAX_VALUE, maxAckPending);
        assertEquals(Integer.MAX_VALUE, maxPullWaiting);
        assertEquals(Integer.MAX_VALUE, maxBatch);
        assertEquals(Integer.MAX_VALUE, maxBytes);
    }
}

