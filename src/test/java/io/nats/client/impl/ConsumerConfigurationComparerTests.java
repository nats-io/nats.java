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

package io.nats.client.impl;

import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;

import static io.nats.client.api.ConsumerConfiguration.CcChangeHelper.*;
import static io.nats.client.support.NatsConstants.EMPTY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerConfigurationComparerTests extends TestBase {

    private void assertNotChange(ConsumerConfiguration original, ConsumerConfiguration server) {
        NatsJetStream.ConsumerConfigurationComparer originalCCC = new NatsJetStream.ConsumerConfigurationComparer(original);
        assertFalse(originalCCC.wouldBeChangeTo(server));
    }

    private void assertChange(ConsumerConfiguration original, ConsumerConfiguration server) {
        NatsJetStream.ConsumerConfigurationComparer originalCCC = new NatsJetStream.ConsumerConfigurationComparer(original);
        assertTrue(originalCCC.wouldBeChangeTo(server));
    }

    private ConsumerConfiguration.Builder builder(ConsumerConfiguration orig) {
        return ConsumerConfiguration.builder(orig);
    }

    @Test
    public void testChanges() {
        ConsumerConfiguration orig = ConsumerConfiguration.builder().build();
        assertNotChange(orig, orig);

        assertNotChange(builder(orig).deliverPolicy(DeliverPolicy.All).build(), orig);
        assertChange(builder(orig).deliverPolicy(DeliverPolicy.New).build(), orig);

        assertNotChange(builder(orig).ackPolicy(AckPolicy.Explicit).build(), orig);
        assertChange(builder(orig).ackPolicy(AckPolicy.None).build(), orig);

        assertNotChange(builder(orig).replayPolicy(ReplayPolicy.Instant).build(), orig);
        assertChange(builder(orig).replayPolicy(ReplayPolicy.Original).build(), orig);

        ConsumerConfiguration ccTest = builder(orig).flowControl(1000).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig);

        ccTest = builder(orig).idleHeartbeat(1000).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig);

        ccTest = builder(orig).maxExpires(1000).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig);

        ccTest = builder(orig).inactiveThreshold(1000).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig);

        ccTest = builder(orig).startTime(ZonedDateTime.now()).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig);

        assertNotChange(builder(orig).headersOnly(false).build(), orig);
        assertChange(builder(orig).headersOnly(true).build(), orig);

        assertNotChange(builder(orig).startSequence(-1).build(), orig);
        assertChange(builder(orig).startSequence(new Long(99)).build(), orig);

        assertNotChange(builder(orig).maxDeliver(-1).build(), orig);
        assertChange(builder(orig).maxDeliver(new Long(99)).build(), orig);

        assertNotChange(builder(orig).rateLimit(-1).build(), orig);
        assertChange(builder(orig).rateLimit(new Long(99)).build(), orig);

        assertNotChange(builder(orig).maxAckPending(0).build(), orig);
        assertChange(builder(orig).maxAckPending(new Long(99)).build(), orig);

        assertNotChange(builder(orig).maxPullWaiting(0).build(), orig);
        assertChange(builder(orig).maxPullWaiting(new Long(99)).build(), orig);

        assertNotChange(builder(orig).maxBatch(-1).build(), orig);
        assertChange(builder(orig).maxBatch(new Long(99)).build(), orig);

        assertNotChange(builder(orig).ackWait(Duration.ofSeconds(30)).build(), orig);
        assertChange(builder(orig).ackWait(Duration.ofSeconds(31)).build(), orig);

        assertNotChange(builder(orig).filterSubject(EMPTY).build(), orig);
        ccTest = builder(orig).filterSubject(PLAIN).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig);

        assertNotChange(builder(orig).description(EMPTY).build(), orig);
        ccTest = builder(orig).description(PLAIN).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig);

        assertNotChange(builder(orig).sampleFrequency(EMPTY).build(), orig);
        ccTest = builder(orig).sampleFrequency(PLAIN).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig);

        assertNotChange(builder(orig).deliverSubject(EMPTY).build(), orig);
        ccTest = builder(orig).deliverSubject(PLAIN).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig);

        assertNotChange(builder(orig).deliverGroup(EMPTY).build(), orig);
        ccTest = builder(orig).deliverGroup(PLAIN).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig);
    }

    @Test
    public void testChangeHelper() {
        // value
        assertFalse(START_SEQ.wouldBeChange(2L, 2L));
        assertFalse(MAX_DELIVER.wouldBeChange(2L, 2L));
        assertFalse(RATE_LIMIT.wouldBeChange(2L, 2L));
        assertFalse(MAX_ACK_PENDING.wouldBeChange(2L, 2L));
        assertFalse(MAX_PULL_WAITING.wouldBeChange(2L, 2L));
        assertFalse(MAX_BATCH.wouldBeChange(2L, 2L));
        assertFalse(ACK_WAIT.wouldBeChange(Duration.ofSeconds(2), Duration.ofSeconds(2)));

        // null
        assertFalse(START_SEQ.wouldBeChange(null, 2L));
        assertFalse(MAX_DELIVER.wouldBeChange(null, 2L));
        assertFalse(RATE_LIMIT.wouldBeChange(null, 2L));
        assertFalse(MAX_ACK_PENDING.wouldBeChange(null, 2L));
        assertFalse(MAX_PULL_WAITING.wouldBeChange(null, 2L));
        assertFalse(MAX_BATCH.wouldBeChange(null, 2L));
        assertFalse(ACK_WAIT.wouldBeChange(null, Duration.ofSeconds(2)));

        // < min vs initial
        assertFalse(START_SEQ.wouldBeChange(-99L, START_SEQ.initial()));
        assertFalse(MAX_DELIVER.wouldBeChange(-99L, MAX_DELIVER.initial()));
        assertFalse(RATE_LIMIT.wouldBeChange(-99L, RATE_LIMIT.initial()));
        assertFalse(MAX_ACK_PENDING.wouldBeChange(-99L, MAX_ACK_PENDING.initial()));
        assertFalse(MAX_PULL_WAITING.wouldBeChange(-99L, MAX_PULL_WAITING.initial()));
        assertFalse(MAX_BATCH.wouldBeChange(-99L, MAX_BATCH.initial()));
        assertFalse(ACK_WAIT.wouldBeChange(Duration.ofSeconds(-99), Duration.ofNanos(ACK_WAIT.initial())));

        // server vs initial
        assertFalse(START_SEQ.wouldBeChange(START_SEQ.server(), START_SEQ.initial()));
        assertFalse(MAX_DELIVER.wouldBeChange(MAX_DELIVER.server(), MAX_DELIVER.initial()));
        assertFalse(RATE_LIMIT.wouldBeChange(RATE_LIMIT.server(), RATE_LIMIT.initial()));
        assertFalse(MAX_ACK_PENDING.wouldBeChange(MAX_ACK_PENDING.server(), MAX_ACK_PENDING.initial()));
        assertFalse(MAX_PULL_WAITING.wouldBeChange(MAX_PULL_WAITING.server(), MAX_PULL_WAITING.initial()));
        assertFalse(MAX_BATCH.wouldBeChange(MAX_BATCH.server(), MAX_BATCH.initial()));
        assertFalse(ACK_WAIT.wouldBeChange(Duration.ofNanos(ACK_WAIT.server()), Duration.ofNanos(ACK_WAIT.initial())));

        assertTrue(START_SEQ.wouldBeChange(1L, null));
        assertTrue(MAX_DELIVER.wouldBeChange(1L, null));
        assertTrue(RATE_LIMIT.wouldBeChange(1L, null));
        assertTrue(MAX_ACK_PENDING.wouldBeChange(1L, null));
        assertTrue(MAX_PULL_WAITING.wouldBeChange(1L, null));
        assertTrue(MAX_BATCH.wouldBeChange(1L, null));
        assertTrue(ACK_WAIT.wouldBeChange(Duration.ofSeconds(1), null));

        assertTrue(START_SEQ.wouldBeChange(1L, 2L));
        assertTrue(MAX_DELIVER.wouldBeChange(1L, 2L));
        assertTrue(RATE_LIMIT.wouldBeChange(1L, 2L));
        assertTrue(MAX_ACK_PENDING.wouldBeChange(1L, 2L));
        assertTrue(MAX_PULL_WAITING.wouldBeChange(1L, 2L));
        assertTrue(MAX_BATCH.wouldBeChange(1L, 2L));
        assertTrue(ACK_WAIT.wouldBeChange(Duration.ofSeconds(1), Duration.ofSeconds(2)));
    }
}
