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

import io.nats.client.StreamInfo;

import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.ApiConstants.STREAMS;

public class StreamListResponse extends ListResponse {
    private final List<StreamInfo> streams;

    public StreamListResponse() {
        this.streams = new ArrayList<>();
    }

    @Override
    public void add(String json) {
        super.add(json);
        List<String> streamInfoJson = JsonUtils.getObjectArray(STREAMS, json);
        for (String j : streamInfoJson) {
            streams.add(new StreamInfo(j));
        }
    }

    public List<StreamInfo> getStreams() {
        return streams;
    }

    public String nextJson() {
        return internalNextJson();
    }
}
