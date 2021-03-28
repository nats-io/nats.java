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

import io.nats.client.impl.JetStreamTestBase;
import org.junit.jupiter.api.Test;

import static io.nats.client.support.JsonUtils.EMPTY_JSON;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AccountStatisticsTests extends JetStreamTestBase {

    @Test
    public void testAccountStatsImpl() {
        String json = dataAsString("AccountStatistics.json");
        AccountStatistics as = new AccountStatistics(getDataMessage(json));
        assertEquals(1, as.getMemory());
        assertEquals(2, as.getStorage());
        assertEquals(3, as.getStreams());
        assertEquals(4, as.getConsumers());

        as = new AccountStatistics(getDataMessage(EMPTY_JSON));
        assertEquals(0, as.getMemory());
        assertEquals(0, as.getStorage());
        assertEquals(0, as.getStreams());
        assertEquals(0, as.getConsumers());

        assertNotNull(as.toString()); // COVERAGE
    }
}
