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

import io.nats.client.impl.NatsJetStream.AccountLimitImpl;
import io.nats.client.impl.NatsJetStream.AccountStatsImpl;
import io.nats.client.support.DebugUtil;
import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static io.nats.client.utils.ResourceUtils.dataAsLines;
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
        AccountLimitImpl ali = new AccountLimitImpl(json);
        assertEquals(1, ali.getMaxMemory());
        assertEquals(2, ali.getMaxStorage());
        assertEquals(3, ali.getMaxStreams());
        assertEquals(4, ali.getMaxConsumers());
    }

    @Test
    public void testAccountStatsImpl() {
        String json = dataAsString("AccountStatsImpl.json");
        AccountStatsImpl asi = new AccountStatsImpl(json);
        assertEquals(1, asi.getMemory());
        assertEquals(2, asi.getStorage());
        assertEquals(3, asi.getStreams());
        assertEquals(4, asi.getConsumers());
    }

    @Test
    public void testConsumerLister() {
        String json = dataAsString("ConsumerLister.json");
        String printable = DebugUtil.printable(new ConsumerLister(json));
        List<String> lines = dataAsLines("ConsumerListPrintable.txt");
        for (String line : lines) {
            assertTrue(printable.contains(line.trim()));
        }

        ConsumerLister cl = new ConsumerLister("{}");
        assertEquals(0, cl.getTotal());
        assertEquals(0, cl.getOffset());
        assertEquals(0, cl.getLimit());
        assertEquals(0, cl.getConsumers().size());
    }

    @Test
    public void testStreamInfo() {
        String json = dataAsString("StreamInfo.json");
        String printable = DebugUtil.printable(new StreamInfo(json));
        List<String> lines = ResourceUtils.resourceAsLines("data/StreamInfoPrintable.txt");
        for (String line : lines) {
            assertTrue(printable.contains(line.trim()));
        }

        StreamInfo si = new StreamInfo("{}");
        assertNull(si.getCreateTime());
        assertNotNull(si.getConfiguration());
        assertNotNull(si.getStreamState());
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
