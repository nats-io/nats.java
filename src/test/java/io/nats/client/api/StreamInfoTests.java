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
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.nats.client.support.JsonUtils.EMPTY_JSON;
import static io.nats.client.support.JsonUtils.printFormatted;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static io.nats.client.utils.TestBase.getDataMessage;
import static org.junit.jupiter.api.Assertions.*;

public class StreamInfoTests {
    static String json = dataAsString("StreamInfo.json");

    @Test
    public void testStreamInfo() {
        StreamInfo si = new StreamInfo(getDataMessage(json));
        validateStreamInfo(si);
        printFormatted(si); // COVERAGE
    }

    private void validateStreamInfo(StreamInfo si) {
        ZonedDateTime zdt = DateTimeUtils.parseDateTime("2021-01-25T20:09:10.6225191Z");
        assertEquals(zdt, si.getCreateTime());

        StreamConfiguration sc = si.getConfiguration();
        assertEquals("streamName", sc.getName());
        assertEquals(3, sc.getSubjects().size());
        assertEquals("sub0", sc.getSubjects().get(0));
        assertEquals("sub1", sc.getSubjects().get(1));
        assertEquals("x.>", sc.getSubjects().get(2));

        assertEquals(RetentionPolicy.Limits, sc.getRetentionPolicy());
        assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
        assertEquals(StorageType.Memory, sc.getStorageType());

        assertNotNull(si.getConfiguration());
        assertNotNull(si.getStreamState());
        assertEquals(1, sc.getMaxConsumers());
        assertEquals(2, sc.getMaxMsgs());
        assertEquals(3, sc.getMaxBytes());
        assertEquals(4, sc.getMaxMsgSize());
        assertEquals(5, sc.getReplicas());

        assertEquals(Duration.ofSeconds(100), sc.getMaxAge());
        assertEquals(Duration.ofSeconds(120), sc.getDuplicateWindow());

        StreamState ss = si.getStreamState();
        assertEquals(11, ss.getMsgCount());
        assertEquals(12, ss.getByteCount());
        assertEquals(13, ss.getFirstSequence());
        assertEquals(14, ss.getLastSequence());

        assertEquals(15, ss.getConsumerCount());
        assertEquals(3, ss.getSubjectCount());
        assertEquals(3, ss.getSubjects().size());

        Map<String, Subject> map = new HashMap<>();
        for (Subject su : ss.getSubjects()) {
            map.put(su.getName(), su);
        }

        Subject s = map.get("sub0");
        assertNotNull(s);
        assertEquals(1, s.getCount());

        s = map.get("sub1");
        assertNotNull(s);
        assertEquals(2, s.getCount());

        s = map.get("x.foo");
        assertNotNull(s);
        assertEquals(3, s.getCount());

        assertEquals(6, ss.getDeletedCount());
        assertEquals(6, ss.getDeleted().size());
        for (long x = 91; x < 97; x++) {
            assertTrue(ss.getDeleted().contains(x));
        }

        assertEquals(DateTimeUtils.parseDateTime("0001-01-01T00:00:00Z"), ss.getFirstTime());
        assertEquals(DateTimeUtils.parseDateTime("0001-01-01T00:00:00Z"), ss.getLastTime());

        Placement pl = si.getConfiguration().getPlacement();
        assertNotNull(pl);
        assertEquals("placementclstr", pl.getCluster());
        assertEquals(2, pl.getTags().size());
        assertEquals("ptag1", pl.getTags().get(0));
        assertEquals("ptag2", pl.getTags().get(1));

        ClusterInfo cli = si.getClusterInfo();
        assertNotNull(cli);
        assertEquals("clustername", cli.getName());
        assertEquals("clusterleader", cli.getLeader());

        assertEquals(2, cli.getReplicas().size());
        assertEquals("name0", cli.getReplicas().get(0).getName());
        assertTrue(cli.getReplicas().get(0).isCurrent());
        assertTrue(cli.getReplicas().get(0).isOffline());
        assertEquals(Duration.ofNanos(230000000000L), cli.getReplicas().get(0).getActive());
        assertEquals(3, cli.getReplicas().get(0).getLag());

        assertEquals("name1", cli.getReplicas().get(1).getName());
        assertFalse(cli.getReplicas().get(1).isCurrent());
        assertFalse(cli.getReplicas().get(1).isOffline());
        assertEquals(Duration.ofNanos(240000000000L), cli.getReplicas().get(1).getActive());
        assertEquals(4, cli.getReplicas().get(1).getLag());

        MirrorInfo mi = si.getMirrorInfo();
        assertNotNull(mi);
        assertEquals("mname", mi.getName());
        assertEquals(16, mi.getLag());
        assertEquals(Duration.ofNanos(160000000000L), mi.getActive());
        assertNull(mi.getError());

        assertEquals(2, si.getSourceInfos().size());
        assertEquals("sname17", si.getSourceInfos().get(0).getName());
        assertEquals(17, si.getSourceInfos().get(0).getLag());
        assertEquals(Duration.ofNanos(170000000000L), si.getSourceInfos().get(0).getActive());
        assertEquals("sname18", si.getSourceInfos().get(1).getName());
        assertEquals(18, si.getSourceInfos().get(1).getLag());
        assertEquals(Duration.ofNanos(180000000000L), si.getSourceInfos().get(1).getActive());

        si = new StreamInfo(EMPTY_JSON);
        assertNull(si.getCreateTime());
        assertNotNull(si.getStreamState());
        assertNotNull(si.getConfiguration());
        assertNull(si.getConfiguration().getPlacement());
        assertNull(si.getClusterInfo());
        assertNull(si.getMirrorInfo());
        assertNull(si.getSourceInfos());

        List<Replica> replicas = Replica.optionalListOf(EMPTY_JSON);
        assertNull(replicas);
    }

    @Test
    public void testToString() {
        // COVERAGE
        assertNotNull(new StreamInfo(json).toString());
    }
}
