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

import java.util.Map;

import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class AccountStatisticsTests extends JetStreamTestBase {

    @Test
    public void testAccountStatsImpl() {
        String json = dataAsString("AccountStatistics.json");
        AccountStatistics as = new AccountStatistics(getDataMessage(json));
        assertEquals(101, as.getMemory());
        assertEquals(102, as.getStorage());
        assertEquals(103, as.getStreams());
        assertEquals(104, as.getConsumers());
        validateAccountLimits(as.getLimits(), 200);

        assertEquals("ngs", as.getDomain());

        ApiStats api = as.getApi();
        assertEquals(301, api.getTotal());
        assertEquals(302, api.getErrors());

        Map<String, AccountTier> tiers = as.getTiers();
        AccountTier tier = tiers.get("R1");
        assertNotNull(tier);
        assertEquals(401, tier.getMemory());
        assertEquals(402, tier.getStorage());
        assertEquals(403, tier.getStreams());
        assertEquals(404, tier.getConsumers());
        validateAccountLimits(tier.getLimits(), 500);

        tier = tiers.get("R3");
        assertNotNull(tier);
        assertEquals(601, tier.getMemory());
        assertEquals(602, tier.getStorage());
        assertEquals(603, tier.getStreams());
        assertEquals(604, tier.getConsumers());
        validateAccountLimits(tier.getLimits(), 700);

        assertNotNull(as.toString()); // COVERAGE

        as = new AccountStatistics(getDataMessage("{}"));
        assertEquals(0, as.getMemory());
        assertEquals(0, as.getStorage());
        assertEquals(0, as.getStreams());
        assertEquals(0, as.getConsumers());

        AccountLimits al = as.getLimits();
        assertNotNull(al);
        assertEquals(0, al.getMaxMemory());
        assertEquals(0, al.getMaxStorage());
        assertEquals(0, al.getMaxStreams());
        assertEquals(0, al.getMaxConsumers());
        assertEquals(0, al.getMaxAckPending());
        assertEquals(0, al.getMemoryMaxStreamBytes());
        assertEquals(0, al.getStorageMaxStreamBytes());
        assertFalse(al.isMaxBytesRequired());

        api = as.getApi();
        assertNotNull(api);
        assertEquals(0, api.getTotal());
        assertEquals(0, api.getErrors());
    }

    private void validateAccountLimits(AccountLimits al, int id) {
        assertEquals(id + 1, al.getMaxMemory());
        assertEquals(id + 2, al.getMaxStorage());
        assertEquals(id + 3, al.getMaxStreams());
        assertEquals(id + 4, al.getMaxConsumers());
        assertEquals(id + 5, al.getMaxAckPending());
        assertEquals(id + 6, al.getMemoryMaxStreamBytes());
        assertEquals(id + 7, al.getStorageMaxStreamBytes());
        assertTrue(al.isMaxBytesRequired());
    }
}
