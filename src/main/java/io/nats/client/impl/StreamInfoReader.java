// Copyright 2022 The NATS Authors
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

import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.StreamInfoOptions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static io.nats.client.support.ApiConstants.DELETED_DETAILS;
import static io.nats.client.support.ApiConstants.SUBJECTS_FILTER;
import static io.nats.client.support.JsonUtils.*;

class StreamInfoReader {

    private final Map<String, StreamInfo> byName;
    private ListRequestEngine engine;

    StreamInfoReader() {
        byName = new HashMap<>();
        engine = new ListRequestEngine();
    }

    void process(Message msg) throws JetStreamApiException {
        engine = new ListRequestEngine(msg);
        addOrMerge(new StreamInfo(new String(msg.getData())));
    }

    boolean hasMore() {
        return engine.hasMore();
    }

    private void addOrMerge(StreamInfo si) {
        String name = si.getConfiguration().getName();
        StreamInfo existing = byName.get(name);
        if (existing == null) {
            byName.put(name, si);
        }
        else {
            existing.getStreamState().addAll(si.getStreamState().getSubjects());
        }
    }

    byte[] nextJson(StreamInfoOptions options) {
        StringBuilder sb = beginJson();
        addField(sb, "offset", engine.nextOffset());
        if (options != null) {
            addField(sb, SUBJECTS_FILTER, options.getSubjectsFilter());
            addFldWhenTrue(sb, DELETED_DETAILS, options.isDeletedDetails());
        }
        return endJson(sb).toString().getBytes();
    }

    Collection<StreamInfo> getStreamInfos() {
        return byName.values();
    }

    StreamInfo getStreamInfo(String streamName) {
        return byName.get(streamName);
    }
}
