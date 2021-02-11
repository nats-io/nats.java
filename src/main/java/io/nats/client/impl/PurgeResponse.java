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

import java.util.regex.Pattern;

public class PurgeResponse {
    private static final Pattern SUCCESS_RE = JsonUtils.buildPattern("success", JsonUtils.FieldType.jsonBoolean);
    private static final Pattern PURGED_RE = JsonUtils.buildPattern("purged", JsonUtils.FieldType.jsonNumber);

    private final boolean success;
    private final int purged;

    public PurgeResponse(String json) {
        success = JsonUtils.readBoolean(json, SUCCESS_RE);
        purged = JsonUtils.readInt(json, PURGED_RE, 0);
    }

    public boolean isSuccess() {
        return success;
    }

    public int getPurged() {
        return purged;
    }

    @Override
    public String toString() {
        return "PurgeResponse{" +
                "success=" + success +
                ", purged=" + purged +
                '}';
    }
}
