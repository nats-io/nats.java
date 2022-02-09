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
}
