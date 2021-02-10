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

import io.nats.client.ConsumerInfo;

import java.util.ArrayList;
import java.util.List;

public class ConsumerListResponse extends ListResponse {
    private final List<ConsumerInfo> consumers;

    public ConsumerListResponse() {
        this.consumers = new ArrayList<>();
    }

    @Override
    public void update(String json) {
        super.update(json);
        List<String> consumersJson = JsonUtils.getObjectArray("consumers", json);
        for (String j : consumersJson) {
            consumers.add(new ConsumerInfo(j));
        }
    }

    public List<ConsumerInfo> getConsumers() {
        return consumers;
    }

    public String nextJson() {
        return internalNextJson();
    }
}
