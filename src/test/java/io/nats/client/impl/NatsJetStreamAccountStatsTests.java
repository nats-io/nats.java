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

import org.junit.jupiter.api.Test;

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NatsJetStreamAccountStatsTests extends JetStreamTestBase {

    @Test
    public void testAccountStatsImpl() {
        String json = dataAsString("AccountStatsImpl.json");
        NatsJetStreamAccountStats asi = new NatsJetStreamAccountStats(json);
        assertEquals(1, asi.getMemory());
        assertEquals(2, asi.getStorage());
        assertEquals(3, asi.getStreams());
        assertEquals(4, asi.getConsumers());

        asi = new NatsJetStreamAccountStats("{}");
        assertEquals(-1, asi.getMemory());
        assertEquals(-1, asi.getStorage());
        assertEquals(-1, asi.getStreams());
        assertEquals(+1, asi.getConsumers());
    }
}