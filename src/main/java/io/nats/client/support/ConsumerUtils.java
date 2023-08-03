// Copyright 2023 The NATS Authors
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

package io.nats.client.support;

import io.nats.client.NUID;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;

public abstract class ConsumerUtils {
    private ConsumerUtils() {}  /* ensures cannot be constructed */

    public static String generateConsumerName() {
        return NUID.nextGlobalSequence();
    }

    public static ConsumerConfiguration nextOrderedConsumerConfiguration(
        ConsumerConfiguration originalCc,
        long lastStreamSeq,
        String newDeliverSubject)
    {
        return ConsumerConfiguration.builder(originalCc)
            .deliverPolicy(DeliverPolicy.ByStartSequence)
            .deliverSubject(newDeliverSubject)
            .startSequence(Math.max(1, lastStreamSeq + 1))
            .startTime(null) // clear start time in case it was originally set
            .build();
    }
}
