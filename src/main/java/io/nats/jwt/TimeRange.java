// Copyright 2021-2024 The NATS Authors
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

package io.nats.jwt;

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;

import java.util.List;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

public class TimeRange implements JsonSerializable {
    public String start;
    public String end;

    static List<TimeRange> optionalListOf(JsonValue jv) {
        return JsonValueUtils.optionalListOf(jv, TimeRange::new);
    }

    public TimeRange(String start, String end) {
        this.start = start;
        this.end = end;
    }

    public TimeRange(JsonValue jv) {
        this.start = JsonValueUtils.readString(jv, "start");
        this.end = JsonValueUtils.readString(jv, "end");
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, "start", start);
        JsonUtils.addField(sb, "end", end);
        return endJson(sb).toString();
    }
}