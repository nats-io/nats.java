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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import static io.nats.client.support.ApiConstants.DELETED_DETAILS;
import static io.nats.client.support.ApiConstants.SUBJECTS_FILTER;
import static io.nats.client.support.JsonUtils.*;

class StreamInfoReader {

    private StreamInfo streamInfo;
    private ListRequestEngine engine;

    StreamInfoReader() {
        engine = new ListRequestEngine();
    }

    void process(@NonNull Message msg) throws JetStreamApiException {
        engine = new ListRequestEngine(msg);
        StreamInfo si = new StreamInfo(msg);
        if (streamInfo == null) {
            streamInfo = si;
        }
        else {
            streamInfo.getStreamState().getSubjects().addAll(si.getStreamState().getSubjects());
        }
    }

    boolean hasMore() {
        return engine.hasMore();
    }

    byte @NonNull [] nextJson(@Nullable StreamInfoOptions options) {
        StringBuilder sb = beginJson();
        addField(sb, "offset", engine.nextOffset());
        if (options != null) {
            addField(sb, SUBJECTS_FILTER, options.getSubjectsFilter());
            addFldWhenTrue(sb, DELETED_DETAILS, options.isDeletedDetails());
        }
        return endJson(sb).toString().getBytes();
    }

    StreamInfo getStreamInfo() {
        return streamInfo;
    }
}
