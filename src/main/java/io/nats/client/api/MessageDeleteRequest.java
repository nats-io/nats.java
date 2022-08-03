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

import static io.nats.client.support.ApiConstants.NO_ERASE;
import static io.nats.client.support.ApiConstants.SEQ;
import static io.nats.client.support.JsonUtils.*;

/**
 * Object used to make a request for message delete requests.
 */
public class MessageDeleteRequest implements JsonSerializable {
    private final long sequence;
    private final boolean noErase;

    public MessageDeleteRequest(long sequence, boolean erase) {
        this.sequence = sequence;
        this.noErase = !erase;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, SEQ, sequence);
        addFldWhenTrue(sb, NO_ERASE, noErase);
        return endJson(sb).toString();
    }
}
