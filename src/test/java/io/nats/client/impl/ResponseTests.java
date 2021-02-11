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

import io.nats.client.ConsumerConfiguration;
import io.nats.client.ConsumerInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class ResponseTests extends JetStreamTestBase {

    @Test
    public void testPurgeResponse() {
        String json = dataAsString("PurgeResponse.json");
        PurgeResponse pr = new PurgeResponse(json);
        assertTrue(pr.isSuccess());
        assertEquals(5, pr.getPurged());
        assertNotNull(pr.toString()); // COVERAGE
    }

    @Test
    public void testConsumerListResponse() {
        String json = dataAsString("ConsumerListResponse.json");
        ConsumerListResponse clr = new ConsumerListResponse();
        clr.add(json);
        assertEquals(2, clr.getConsumers().size());

        ConsumerInfo ci = clr.getConsumers().get(0);
        assertEquals("stream-1", ci.getStreamName());
        assertEquals("cname1", ci.getName());
        Assertions.assertEquals(DateTimeUtils.parseDateTime("2021-01-20T23:41:08.579594Z"), ci.getCreationTime());
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

        clr = new ConsumerListResponse();
        clr.add("{}");
        assertEquals(0, clr.getConsumers().size());
    }

    @Test
    public void testStreamListResponse() {
        String json = dataAsString("StreamListResponse.json");
        StreamListResponse slr = new StreamListResponse();
        slr.add(json);
        assertEquals(2, slr.getStreams().size());
        assertEquals("stream-0", slr.getStreams().get(0).getConfiguration().getName());
        assertEquals("stream-1", slr.getStreams().get(1).getConfiguration().getName());
    }

    static class TestListResponse extends ListResponse {
        public int getTotal() { return total; }
        public int getLimit() { return limit; }
        public int getLastOffset() { return lastOffset; }

        public int addCalled = 0;

        @Override
        public void add(String json) {
            super.add(json);
            addCalled++;
        }
    }

    @Test
    public void testListResponseCoverage() {
        TestListResponse tlr = new TestListResponse();
        assertTrue(tlr.hasMore());
        assertEquals("{\"offset\":0}", tlr.internalNextJson());
        assertEquals("{\"offset\":0}", tlr.internalNextJson("name", null));
        assertEquals("{\"offset\":0,\"name\":\"value\"}", tlr.internalNextJson("name", "value"));
        tlr.add(dataAsString("ListResponsePage1.json"));
        assertEquals(1, tlr.addCalled);
        assertEquals(15, tlr.getTotal());
        assertEquals(10, tlr.getLimit());
        assertEquals(0, tlr.getLastOffset());

        assertTrue(tlr.hasMore());
        assertEquals("{\"offset\":10}", tlr.internalNextJson());
        assertEquals("{\"offset\":10,\"name\":\"value\"}", tlr.internalNextJson("name", "value"));
        tlr.add(dataAsString("ListResponsePage2.json"));
        assertEquals(2, tlr.addCalled);
        assertEquals(15, tlr.getTotal());
        assertEquals(10, tlr.getLimit());
        assertEquals(10, tlr.getLastOffset());

        assertFalse(tlr.hasMore());
        assertNull(tlr.internalNextJson());
        assertNull(tlr.internalNextJson("name", "value"));
    }
}
