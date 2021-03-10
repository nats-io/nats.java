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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.nats.client.support.ApiConstants.CONSUMERS;
import static io.nats.client.support.ApiConstants.SUBJECT;

public class ConsumerNamesResponse extends ListResponse {
    private final List<String> consumers;

    ConsumerNamesResponse() {
        consumers = new ArrayList<>();
    }

    @Override
    void add(String json) {
        super.add(json);
        List<String> up = Arrays.asList(JsonUtils.getStringArray(CONSUMERS, json));
        consumers.addAll(up);
    }

    /**
     * Get the list of consumer names
     * @return the list of consumer names
     */
    public List<String> getConsumers() {
        return consumers;
    }

    byte[] nextJson(String filter) {
        return internalNextJson(SUBJECT, filter);
    }
}
