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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static io.nats.client.support.JsonUtils.EMPTY_JSON;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class ConsumerInfoTests {

    @Test
    public void testTime() {
        String json = dataAsString("ConsumerInfo.json");
        long start = System.currentTimeMillis();
        for (int x = 0; x < 1_000_000; x++) {
            new ConsumerInfo(json);
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void testConsumerInfo() {
        String json = dataAsString("ConsumerInfo.json");
        ConsumerInfo ci = new ConsumerInfo(json);
        assertEquals("foo-stream", ci.getStreamName());
        assertEquals("foo-consumer", ci.getName());

        SequencePair sp = ci.getDelivered();
        assertEquals(1, sp.getConsumerSequence());
        assertEquals(2, sp.getStreamSequence());
        assertTrue(sp.toString().contains("SequenceInfo")); // coverage

        //noinspection CastCanBeRemovedNarrowingVariableType
        SequenceInfo sinfo = (SequenceInfo)sp;
        assertEquals(1, sinfo.getConsumerSequence());
        assertEquals(2, sinfo.getStreamSequence());
        assertEquals(DateTimeUtils.parseDateTime("2022-06-29T19:33:21.163377Z"), sinfo.getLastActive());

        sp = ci.getAckFloor();
        assertEquals(3, sp.getConsumerSequence());
        assertEquals(4, sp.getStreamSequence());

        //noinspection CastCanBeRemovedNarrowingVariableType
        sinfo = (SequenceInfo)sp;
        assertEquals(3, sinfo.getConsumerSequence());
        assertEquals(4, sinfo.getStreamSequence());
        assertEquals(DateTimeUtils.parseDateTime("2022-06-29T20:33:21.163377Z"), sinfo.getLastActive());

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

        ClusterInfo clusterInfo = ci.getClusterInfo();
        assertNotNull(clusterInfo);
        assertEquals("clustername", clusterInfo.getName());
        assertEquals("clusterleader", clusterInfo.getLeader());
        List<Replica> reps = clusterInfo.getReplicas();
        assertNotNull(reps);
        assertEquals(2, reps.size());

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
