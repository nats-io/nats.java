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

import io.nats.client.Message;
import io.nats.client.api.*;
import io.nats.client.support.DateTimeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.JsonUtils.EMPTY_JSON;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class ResponseTests extends JetStreamTestBase {

    @Test
    public void testPurgeResponse() {
        String json = dataAsString("PurgeResponse.json");
        PurgeResponse pr = new PurgeResponse(getDataMessage(json));
        assertTrue(pr.isSuccess());
        assertEquals(5, pr.getPurgedCount());
        assertNotNull(pr.toString()); // COVERAGE
    }

    @Test
    public void testConsumerListResponse() {
        String json = dataAsString("ConsumerListResponse.json");
        ConsumerListResponse clr = new ConsumerListResponse(getDataMessage(json));
        List<ConsumerInfo> list = new ArrayList<>();
        clr.addTo(list);
        assertEquals(2, list.size());

        ConsumerInfo ci = list.get(0);
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
        assertEquals(DeliverPolicy.All, cc.getDeliverPolicy());
        assertEquals(AckPolicy.Explicit, cc.getAckPolicy());
        assertEquals(Duration.ofSeconds(30), cc.getAckWait());
        assertEquals(99, cc.getMaxDeliver());
        assertEquals(ReplayPolicy.Instant, cc.getReplayPolicy());

        SequencePair sp = ci.getDelivered();
        assertEquals(1, sp.getConsumerSequence());
        assertEquals(2, sp.getStreamSequence());

        sp = ci.getAckFloor();
        assertEquals(3, sp.getConsumerSequence());
        assertEquals(4, sp.getStreamSequence());

        clr = new ConsumerListResponse(getDataMessage(EMPTY_JSON));
        list.clear();
        clr.addTo(list);
        assertEquals(0, list.size());
    }

    @Test
    public void testStreamListResponse() {
        String json = dataAsString("StreamListResponse.json");
        StreamListResponse slr = new StreamListResponse(getDataMessage(json));
        List<StreamInfo> list = new ArrayList<>();
        slr.addTo(list);
        assertEquals(2, list.size());
        assertEquals("stream-0", list.get(0).getConfiguration().getName());
        assertEquals("stream-1", list.get(1).getConfiguration().getName());
    }

    static class TestListResponse extends ListResponse<TestListResponse> {
        public int getTotal() { return total; }
        public int getLimit() { return limit; }
        public int getLastOffset() { return lastOffset; }

        public TestListResponse() {}

        public TestListResponse(Message msg) {
            super(msg);
        }
    }

    @Test
    public void testListResponseCoverage() {
        TestListResponse tlr = new TestListResponse();
        assertTrue(tlr.hasMore());
        assertEquals("{\"offset\":0}", new String(tlr.internalNextJson()));
        assertEquals("{\"offset\":0}", new String(tlr.internalNextJson("name", null)));
        assertEquals("{\"offset\":0,\"name\":\"value\"}", new String(tlr.internalNextJson("name", "value")));
        tlr = new TestListResponse(getDataMessage(dataAsString("ListResponsePage1.json")));
        assertEquals(15, tlr.getTotal());
        assertEquals(10, tlr.getLimit());
        assertEquals(0, tlr.getLastOffset());

        assertTrue(tlr.hasMore());
        assertEquals("{\"offset\":10}", new String(tlr.internalNextJson()));
        assertEquals("{\"offset\":10,\"name\":\"value\"}", new String(tlr.internalNextJson("name", "value")));
        tlr = new TestListResponse(getDataMessage(dataAsString("ListResponsePage2.json")));
        assertEquals(15, tlr.getTotal());
        assertEquals(10, tlr.getLimit());
        assertEquals(10, tlr.getLastOffset());

        assertFalse(tlr.hasMore());
        assertNull(tlr.internalNextJson());
        assertNull(tlr.internalNextJson("name", "value"));
    }
}
