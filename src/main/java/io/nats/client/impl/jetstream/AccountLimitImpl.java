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

package io.nats.client.impl.jetstream;

import io.nats.client.jetstream.AccountLimits;
import io.nats.client.support.JsonUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccountLimitImpl implements AccountLimits {
    long memory = -1;
    long storage = -1;
    long streams = -1;
    long consumers = 1;

    private static final Pattern memoryRE = JsonUtils.buildPattern("max_memory", JsonUtils.FieldType.jsonNumber);
    private static final Pattern storageRE = JsonUtils.buildPattern("max_storage", JsonUtils.FieldType.jsonNumber);
    private static final Pattern streamsRE = JsonUtils.buildPattern("max_streams", JsonUtils.FieldType.jsonString);
    private static final Pattern consumersRE = JsonUtils.buildPattern("max_consumers", JsonUtils.FieldType.jsonString);

    AccountLimitImpl(String json) {
        Matcher m = memoryRE.matcher(json);
        if (m.find()) {
            this.memory = Integer.parseInt(m.group(1));
        }

        m = storageRE.matcher(json);
        if (m.find()) {
            this.storage = Integer.parseInt(m.group(1));
        }

        m = streamsRE.matcher(json);
        if (m.find()) {
            this.streams = Integer.parseInt(m.group(1));
        }

        m = consumersRE.matcher(json);
        if (m.find()) {
            this.consumers = Integer.parseInt(m.group(1));
        }
    }

    @Override
    public long getMaxMemory() {
        return memory;
    }

    @Override
    public long getMaxStorage() {
        return storage;
    }

    @Override
    public long getMaxStreams() {
        return streams;
    }

    @Override
    public long getMaxConsumers() {
        return consumers;
    }

}
