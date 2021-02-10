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

import io.nats.client.impl.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConsumerLister {
    private int total;
    private int offset;
    private int limit;
    private final List<ConsumerInfo> consumers;

    private static final Pattern totalRE = JsonUtils.buildPattern("total", JsonUtils.FieldType.jsonNumber);
    private static final Pattern offsetRE = JsonUtils.buildPattern("offset", JsonUtils.FieldType.jsonNumber);
    private static final Pattern limitRE = JsonUtils.buildPattern("limit", JsonUtils.FieldType.jsonNumber);

    public ConsumerLister(String json) {
        Matcher m = totalRE.matcher(json);
        if (m.find()) {
            this.total = Integer.parseInt(m.group(1));
        }

        m = offsetRE.matcher(json);
        if (m.find()) {
            this.offset = Integer.parseInt(m.group(1));
        }

        m = limitRE.matcher(json);
        if (m.find()) {
            this.limit = Integer.parseInt(m.group(1));
        }

        this.consumers = new ArrayList<>();
        List<String> consumersJson = JsonUtils.getJSONArray("consumers", json);
        for (String j : consumersJson) {
            this.consumers.add(new ConsumerInfo(j));
        }
    }

    public int getTotal() {
        return total;
    }

    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

    public List<ConsumerInfo> getConsumers() {
        return consumers;
    }

    @Override
    public String toString() {
        return "ConsumerLister{" +
                "total=" + total +
                ", offset=" + offset +
                ", limit=" + limit +
                ", " + consumers +
                '}';
    }
}
