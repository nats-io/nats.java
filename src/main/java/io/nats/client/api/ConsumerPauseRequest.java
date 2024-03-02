// Copyright 2024 The NATS Authors
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
import io.nats.client.support.JsonUtils;
import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.CONFIG;
import static io.nats.client.support.ApiConstants.PAUSE_UNTIL;
import static io.nats.client.support.ApiConstants.STREAM_NAME;
import static io.nats.client.support.JsonUtils.addField;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

/**
 * Object used to make a request to pause a consumer. Used Internally
 */
public class ConsumerPauseRequest implements JsonSerializable {
    private final ZonedDateTime pauseUntil;

    public ConsumerPauseRequest(ZonedDateTime pauseUntil) {
        this.pauseUntil = pauseUntil;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();

        addField(sb, PAUSE_UNTIL, pauseUntil);

        return endJson(sb).toString();
    }

    @Override
    public String toString() {
        return "ConsumerPauseRequest{" +
                "pauseUntil='" + pauseUntil + "'" +
                '}';
    }
}
