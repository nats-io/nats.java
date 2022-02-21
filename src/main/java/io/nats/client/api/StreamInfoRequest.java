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

import io.nats.client.support.JsonSerializable;

import static io.nats.client.support.ApiConstants.SUBJECTS_FILTER;
import static io.nats.client.support.JsonUtils.*;

/**
 * Object used to make a request for special stream info requests
 */
public class StreamInfoRequest implements JsonSerializable {
    private final String subjectsFilter;

    public static byte[] filterSubjects(String subjectsFilter) {
        return new StreamInfoRequest(subjectsFilter).serialize();
    }

    public static byte[] allSubjects() {
        return new StreamInfoRequest(">").serialize();
    }

    public StreamInfoRequest(String subjectsFilter) {
        this.subjectsFilter = subjectsFilter;

    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, SUBJECTS_FILTER, subjectsFilter);
        return endJson(sb).toString();
    }
}
