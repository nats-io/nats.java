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

import io.nats.client.impl.JsonUtils;
import io.nats.client.impl.NatsJetStream;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamApiObjectsTests {
    
	@Test
    public void testOptions() {
        JetStreamOptions jo = JetStreamOptions.builder().requestTimeout(Duration.ofSeconds(42)).prefix("pre").direct(true).build();
        assertEquals("pre", jo.getPrefix());
        assertEquals(Duration.ofSeconds(42), jo.getRequestTimeout());
        assertTrue(jo.isDirectMode());
    }

    @Test
    public void testInvalidPrefix() {
        assertThrows(IllegalArgumentException.class, () -> { JetStreamOptions.builder().prefix(">").build();});
        assertThrows(IllegalArgumentException.class, () -> { JetStreamOptions.builder().prefix("*").build();});
    }

    @Test
    public void testAccountLimitImpl() {
        String json = dataAsString("AccountLimitImpl.json");
        NatsJetStream.AccountLimitImpl ali = new NatsJetStream.AccountLimitImpl(json);
        assertEquals(1, ali.getMaxMemory());
        assertEquals(2, ali.getMaxStorage());
        assertEquals(3, ali.getMaxStreams());
        assertEquals(4, ali.getMaxConsumers());
    }

    @Test
    public void testAccountStatsImpl() {
        String json = dataAsString("AccountStatsImpl.json");
        NatsJetStream.AccountStatsImpl asi = new NatsJetStream.AccountStatsImpl(json);
        assertEquals(1, asi.getMemory());
        assertEquals(2, asi.getStorage());
        assertEquals(3, asi.getStreams());
        assertEquals(4, asi.getConsumers());
    }

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
        assertEquals(JsonUtils.parseDateTime("2021-01-20T23:41:08.579594Z"), ci.getCreationTime());
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

    @Test
    public void testStreamInfo() {
        String json = dataAsString("StreamInfo.json");
        StreamInfo si = new StreamInfo(json);
        assertEquals(JsonUtils.parseDateTime("2021-01-25T20:09:10.6225191Z"), si.getCreateTime());

        StreamConfiguration sc = si.getConfiguration();
        assertEquals("streamName", sc.getName());
        assertEquals(2, sc.getSubjects().length);
        assertEquals("sub0", sc.getSubjects()[0]);
        assertEquals("sub1", sc.getSubjects()[1]);

        assertEquals(StreamConfiguration.RetentionPolicy.Limits, sc.getRetentionPolicy());
        assertEquals(StreamConfiguration.DiscardPolicy.Old, sc.getDiscardPolicy());
        assertEquals(StreamConfiguration.StorageType.Memory, sc.getStorageType());

        assertNotNull(si.getConfiguration());
        assertNotNull(si.getStreamState());
        assertEquals(1, sc.getMaxConsumers());
        assertEquals(2, sc.getMaxMsgs());
        assertEquals(3, sc.getMaxBytes());
        assertEquals(4, sc.getMaxMsgSize());
        assertEquals(5, sc.getReplicas());

        assertEquals(Duration.ofSeconds(100), sc.getMaxAge());
        assertEquals(Duration.ofSeconds(120), sc.getDuplicateWindow());

        StreamInfo.StreamState ss = si.getStreamState();
        assertEquals(11, ss.getMsgCount());
        assertEquals(12, ss.getByteCount());
        assertEquals(13, ss.getFirstSequence());
        assertEquals(14, ss.getLastSequence());
        assertEquals(15, ss.getConsumerCount());

        assertEquals(JsonUtils.parseDateTime("0001-01-01T00:00:00Z"), ss.getFirstTime());
        assertEquals(JsonUtils.parseDateTime("0001-01-01T00:00:00Z"), ss.getLastTime());

        si = new StreamInfo("{}");
        assertNull(si.getCreateTime());
        assertNotNull(si.getStreamState());
        assertNotNull(si.getConfiguration());
    }

    @Test
    public void testConsumerInfo() {
        ConsumerInfo ci = new ConsumerInfo("{}");
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
}
