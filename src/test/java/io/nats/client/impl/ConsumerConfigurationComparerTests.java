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
import java.util.List;

import static io.nats.client.api.ConsumerConfiguration.*;
import static io.nats.client.api.ConsumerConfiguration.LongChangeHelper.MAX_DELIVER;
import static io.nats.client.support.NatsConstants.EMPTY;
import static org.junit.jupiter.api.Assertions.*;

public class ConsumerConfigurationComparerTests extends TestBase {

    private void assertNotChange(ConsumerConfiguration original, ConsumerConfiguration server) {
        NatsJetStream.ConsumerConfigurationComparer originalCCC = new NatsJetStream.ConsumerConfigurationComparer(original);
        assertEquals(0, originalCCC.getChanges(server).size());
    }

    private void assertChange(ConsumerConfiguration original, ConsumerConfiguration server, String... changeFields) {
        NatsJetStream.ConsumerConfigurationComparer originalCCC = new NatsJetStream.ConsumerConfigurationComparer(original);
        List<String> changes = originalCCC.getChanges(server);
        assertEquals(changeFields.length, changes.size());
        for (String ch : changeFields) {
            assertTrue(changes.contains(ch));
        }
    }

    private Builder builder(ConsumerConfiguration orig) {
        return ConsumerConfiguration.builder(orig);
    }

    @Test
    public void testChangeFieldsIdentified() {
        ConsumerConfiguration orig = ConsumerConfiguration.builder().build();

        assertNotChange(orig, orig);

        assertNotChange(builder(orig).deliverPolicy(DeliverPolicy.All).build(), orig);
        assertChange(builder(orig).deliverPolicy(DeliverPolicy.New).build(), orig, "deliverPolicy");

        assertNotChange(builder(orig).ackPolicy(AckPolicy.Explicit).build(), orig);
        assertChange(builder(orig).ackPolicy(AckPolicy.None).build(), orig, "ackPolicy");

        assertNotChange(builder(orig).replayPolicy(ReplayPolicy.Instant).build(), orig);
        assertChange(builder(orig).replayPolicy(ReplayPolicy.Original).build(), orig, "replayPolicy");

        ConsumerConfiguration ccTest = builder(orig).flowControl(1000).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "flowControl", "idleHeartbeat");

        ccTest = builder(orig).idleHeartbeat(1000).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "idleHeartbeat");

        ccTest = builder(orig).maxExpires(1000).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "maxExpires");

        ccTest = builder(orig).inactiveThreshold(1000).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "inactiveThreshold");

        ccTest = builder(orig).startTime(ZonedDateTime.now()).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "startTime");

        assertNotChange(builder(orig).headersOnly(false).build(), orig);
        assertChange(builder(orig).headersOnly(true).build(), orig, "headersOnly");

        assertNotChange(builder(orig).startSequence(UNSET_ULONG).build(), orig);
        assertNotChange(builder(orig).startSequence(null).build(), orig);
        assertChange(builder(orig).startSequence(1).build(), orig, "startSequence");

        assertNotChange(builder(orig).maxDeliver(MAX_DELIVER.Unset).build(), orig);
        assertNotChange(builder(orig).maxDeliver(null).build(), orig);
        assertChange(builder(orig).maxDeliver(MAX_DELIVER.Min).build(), orig, "maxDeliver");

        assertNotChange(builder(orig).rateLimit(UNSET_ULONG).build(), orig);
        assertNotChange(builder(orig).rateLimit(null).build(), orig);
        assertChange(builder(orig).rateLimit(1).build(), orig, "rateLimit");

        assertNotChange(builder(orig).maxAckPending(UNSET_LONG).build(), orig);
        assertNotChange(builder(orig).maxAckPending(null).build(), orig);
        assertChange(builder(orig).maxAckPending(1).build(), orig, "maxAckPending");

        assertNotChange(builder(orig).maxPullWaiting(UNSET_LONG).build(), orig);
        assertNotChange(builder(orig).maxPullWaiting(null).build(), orig);
        assertChange(builder(orig).maxPullWaiting(1).build(), orig, "maxPullWaiting");

        assertNotChange(builder(orig).maxBatch(UNSET_LONG).build(), orig);
        assertNotChange(builder(orig).maxBatch(null).build(), orig);
        assertChange(builder(orig).maxBatch(1).build(), orig, "maxBatch");

        assertNotChange(builder(orig).ackWait(DURATION_UNSET_LONG).build(), orig);
        assertNotChange(builder(orig).ackWait(null).build(), orig);
        assertChange(builder(orig).ackWait(1).build(), orig, "ackWait");

        assertNotChange(builder(orig).filterSubject(EMPTY).build(), orig);
        ccTest = builder(orig).filterSubject(PLAIN).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "filterSubject");

        assertNotChange(builder(orig).description(EMPTY).build(), orig);
        ccTest = builder(orig).description(PLAIN).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "description");

        assertNotChange(builder(orig).sampleFrequency(EMPTY).build(), orig);
        ccTest = builder(orig).sampleFrequency(PLAIN).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "sampleFrequency");

        assertNotChange(builder(orig).deliverSubject(EMPTY).build(), orig);
        ccTest = builder(orig).deliverSubject(PLAIN).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "deliverSubject");

        assertNotChange(builder(orig).deliverGroup(EMPTY).build(), orig);
        ccTest = builder(orig).deliverGroup(PLAIN).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "deliverGroup");

        Duration[] nodurs = null;
        assertNotChange(builder(orig).backoff(nodurs).build(), orig);
        assertNotChange(builder(orig).backoff((Duration)null).build(), orig);
        assertNotChange(builder(orig).backoff(new Duration[0]).build(), orig);
        ccTest = builder(orig).backoff(1000, 2000).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "backoff");

        long[] nolongs = null;
        assertNotChange(builder(orig).backoff(nolongs).build(), orig);
        assertNotChange(builder(orig).backoff(new long[0]).build(), orig);
        ccTest = builder(orig).backoff(1000, 2000).build();
        assertNotChange(ccTest, ccTest);
        assertChange(ccTest, orig, "backoff");
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testChangeHelpers() {
        for (LongChangeHelper h : LongChangeHelper.values()) {
            assertFalse(h.wouldBeChange(h.Min, h.Min));    // has value vs server has same value
            assertTrue(h.wouldBeChange(h.Min, h.Min + 1)); // has value vs server has different value

            assertFalse(h.wouldBeChange(null, h.Min));       // value not set vs server has value
            assertFalse(h.wouldBeChange(null, h.Unset));     // value not set vs server has unset value

            assertTrue(h.wouldBeChange(h.Min, null));        // has value vs server not set
            assertFalse(h.wouldBeChange(h.Unset, null));     // has unset value versus server not set
        }
    }
}
