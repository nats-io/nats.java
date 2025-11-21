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

import static io.nats.client.support.JsonUtils.EMPTY_JSON;
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
        assertEquals(105, as.getReservedMemory());
        assertEquals(106, as.getReservedStorage());
        validateAccountLimits(as.getLimits(), 200);

        assertEquals("ngs", as.getDomain());

        ApiStats api = as.getApi();
        //noinspection deprecation
        assertEquals(301, api.getTotal()); // COVERAGE
        //noinspection deprecation
        assertEquals(302, api.getErrors()); // COVERAGE
        assertEquals(301, api.getTotalApiRequests());
        assertEquals(302, api.getErrorCount());
        assertEquals(303, api.getLevel());
        assertEquals(304, api.getInFlight());

        Map<String, AccountTier> tiers = as.getTiers();
        validateTier(tiers.get("R1"), 400, 500);
        validateTier(tiers.get("R3"), 600, 700);

        assertNotNull(as.toString()); // COVERAGE

        as = new AccountStatistics(getDataMessage(EMPTY_JSON));
        assertEquals(0, as.getMemory());
        assertEquals(0, as.getStorage());
        assertEquals(0, as.getStreams());
        assertEquals(0, as.getConsumers());

        AccountLimits al = as.getLimits();
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
        //noinspection deprecation
        assertEquals(0, api.getTotal()); // COVERAGE
        //noinspection deprecation
        assertEquals(0, api.getErrors()); // COVERAGE
        assertEquals(0, api.getTotalApiRequests());
        assertEquals(0, api.getErrorCount());
        assertEquals(0, api.getLevel());
        assertEquals(0, api.getInFlight());
    }

    private void validateTier(AccountTier tier, int tierBase, int limitsIdBase) {
        assertNotNull(tier);
        //noinspection deprecation
        assertEquals(tierBase + 1, tier.getMemory()); // COVERAGE
        assertEquals(tierBase + 1, tier.getMemoryBytes());
        //noinspection deprecation
        assertEquals(tierBase + 2, tier.getStorage()); // COVERAGE
        assertEquals(tierBase + 2, tier.getStorageBytes());
        assertEquals(tierBase + 3, tier.getStreams());
        assertEquals(tierBase + 4, tier.getConsumers());
        assertEquals(tierBase + 5, tier.getReservedMemory());
        assertEquals(tierBase + 6, tier.getReservedStorage());
        validateAccountLimits(tier.getLimits(), limitsIdBase);
    }

    private static void validateAccountLimits(AccountLimits al, int limitsIdBase) {
        assertNotNull(al);
        assertEquals(limitsIdBase + 1, al.getMaxMemory());
        assertEquals(limitsIdBase + 2, al.getMaxStorage());
        assertEquals(limitsIdBase + 3, al.getMaxStreams());
        assertEquals(limitsIdBase + 4, al.getMaxConsumers());
        assertEquals(limitsIdBase + 5, al.getMaxAckPending());
        assertEquals(limitsIdBase + 6, al.getMemoryMaxStreamBytes());
        assertEquals(limitsIdBase + 7, al.getStorageMaxStreamBytes());
        assertTrue(al.isMaxBytesRequired());
    }
}
