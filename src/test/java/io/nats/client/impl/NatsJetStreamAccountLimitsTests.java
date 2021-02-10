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

public class NatsJetStreamAccountLimitsTests extends JetStreamTestBase {

    @Test
    public void testAccountLimitImpl() {
        String json = dataAsString("AccountLimitImpl.json");
        NatsJetStreamAccountLimits ali = new NatsJetStreamAccountLimits(json);
        assertEquals(1, ali.getMaxMemory());
        assertEquals(2, ali.getMaxStorage());
        assertEquals(3, ali.getMaxStreams());
        assertEquals(4, ali.getMaxConsumers());

        ali = new NatsJetStreamAccountLimits("{}");
        assertEquals(-1, ali.getMaxMemory());
        assertEquals(-1, ali.getMaxStorage());
        assertEquals(-1, ali.getMaxStreams());
        assertEquals(+1, ali.getMaxConsumers());
    }
}