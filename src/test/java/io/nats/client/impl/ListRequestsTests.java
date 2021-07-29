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

import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.*;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.Ulong;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static io.nats.client.support.JsonUtils.EMPTY_JSON;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class ListRequestsTests extends JetStreamTestBase {

    @Test
    public void testConsumerListResponse() throws Exception {
        String json = dataAsString("ConsumerListResponse.json");
        ConsumerListReader clr = new ConsumerListReader();
        clr.process(getDataMessage(json));
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
        assertEquals(DeliverPolicy.All, cc.getDeliverPolicy());
        assertEquals(AckPolicy.Explicit, cc.getAckPolicy());
        assertEquals(Duration.ofSeconds(30), cc.getAckWait());
        assertEquals(99, cc.getMaxDeliver());
        assertEquals(ReplayPolicy.Instant, cc.getReplayPolicy());

        SequencePair sp = ci.getDelivered();
        assertEquals(new Ulong(1), sp.getConsumerSequenceNum());
        assertEquals(new Ulong(2), sp.getStreamSequenceNum());

        sp = ci.getAckFloor();
        assertEquals(new Ulong(3), sp.getConsumerSequenceNum());
        assertEquals(new Ulong(4), sp.getStreamSequenceNum());

        clr = new ConsumerListReader();
        clr.process(getDataMessage(EMPTY_JSON));
        assertEquals(0, clr.getConsumers().size());
    }

    @Test
    public void testStreamListResponse() throws Exception {
        String json = dataAsString("StreamListResponse.json");
        StreamListReader slr = new StreamListReader();
        slr.process(getDataMessage(json));
        assertEquals(2, slr.getStreams().size());
        assertEquals("stream-0", slr.getStreams().get(0).getConfiguration().getName());
        assertEquals("stream-1", slr.getStreams().get(1).getConfiguration().getName());
    }

    static class TestListRequestEngine extends ListRequestEngine {
        public int getTotal() { return total; }
        public int getLimit() { return limit; }
        public int getLastOffset() { return lastOffset; }

        public TestListRequestEngine() {}

        public TestListRequestEngine(Message msg) throws JetStreamApiException {
            super(msg);
        }
    }

    @Test
    public void testListRequestEngine() throws Exception {
        TestListRequestEngine tlr = new TestListRequestEngine();
        assertTrue(tlr.hasMore());
        assertEquals("{\"offset\":0}", new String(tlr.internalNextJson()));
        assertEquals("{\"offset\":0}", new String(tlr.internalNextJson("name", null)));
        assertEquals("{\"offset\":0,\"name\":\"value\"}", new String(tlr.internalNextJson("name", "value")));
        tlr = new TestListRequestEngine(getDataMessage(dataAsString("ListResponsePage1.json")));
        assertEquals(15, tlr.getTotal());
        assertEquals(10, tlr.getLimit());
        assertEquals(0, tlr.getLastOffset());

        assertTrue(tlr.hasMore());
        assertEquals("{\"offset\":10}", new String(tlr.internalNextJson()));
        assertEquals("{\"offset\":10,\"name\":\"value\"}", new String(tlr.internalNextJson("name", "value")));
        tlr = new TestListRequestEngine(getDataMessage(dataAsString("ListResponsePage2.json")));
        assertEquals(15, tlr.getTotal());
        assertEquals(10, tlr.getLimit());
        assertEquals(10, tlr.getLastOffset());

        assertFalse(tlr.hasMore());
        assertNull(tlr.internalNextJson());
        assertNull(tlr.internalNextJson("name", "value"));

        String json = dataAsString("GenericErrorResponse.json");
        NatsMessage m = new NatsMessage("sub", null, json.getBytes(StandardCharsets.US_ASCII));
        assertThrows(JetStreamApiException.class, () -> new ListRequestEngine(m));
    }
}
