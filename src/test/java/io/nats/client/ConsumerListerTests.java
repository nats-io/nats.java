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

import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.impl.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsumerListerTests extends JetStreamTestBase {

    @Test
    public void testConsumerLister() {
        String json = dataAsString("ConsumerLister.json");
        ConsumerLister cl = new ConsumerLister(json);
        assertEquals(2, cl.getTotal());
        assertEquals(42, cl.getOffset());
        assertEquals(256, cl.getLimit());
        assertEquals(2, cl.getConsumers().size());

        ConsumerInfo ci = cl.getConsumers().get(0);
        assertEquals("stream-1", ci.getStreamName());
        assertEquals("cname1", ci.getName());
        Assertions.assertEquals(JsonUtils.parseDateTime("2021-01-20T23:41:08.579594Z"), ci.getCreationTime());
        assertEquals(5, ci.getNumAckPending());
        assertEquals(6, ci.getRedelivered());
        assertEquals(7, ci.getNumWaiting());
        assertEquals(8, ci.getNumPending());

        ConsumerConfiguration cc = ci.getConsumerConfiguration();
        assertEquals("cname1", cc.getDurable());
        assertEquals("strm1-deliver", cc.getDeliverSubject());
        assertEquals(ConsumerConfiguration.DeliverPolicy.All, cc.getDeliverPolicy());
        assertEquals(ConsumerConfiguration.AckPolicy.Explicit, cc.getAckPolicy());
        assertEquals(Duration.ofSeconds(30), cc.getAckWait());
        assertEquals(99, cc.getMaxDeliver());
        assertEquals(ConsumerConfiguration.ReplayPolicy.Instant, cc.getReplayPolicy());

        ConsumerInfo.SequencePair sp = ci.getDelivered();
        assertEquals(1, sp.getConsumerSequence());
        assertEquals(2, sp.getStreamSequence());

        sp = ci.getAckFloor();
        assertEquals(3, sp.getConsumerSequence());
        assertEquals(4, sp.getStreamSequence());

        cl = new ConsumerLister("{}");
        assertEquals(0, cl.getTotal());
        assertEquals(0, cl.getOffset());
        assertEquals(0, cl.getLimit());
        assertEquals(0, cl.getConsumers().size());
    }
}