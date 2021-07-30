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

import io.nats.client.Message;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.Ulong;

import static io.nats.client.support.ApiConstants.PURGED_RE;
import static io.nats.client.support.ApiConstants.SUCCESS_RE;

public class PurgeResponse extends ApiResponse<PurgeResponse> {

    private final boolean success;
    private final Ulong purged;

    public PurgeResponse(Message msg) {
        super(msg);
        success = JsonUtils.readBoolean(json, SUCCESS_RE);
        purged = JsonUtils.readUlong(json, PURGED_RE, Ulong.ZERO);
    }

    /**
     * Returns true if the server was able to purge the stream
     * @return the result flag
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Returns the number of items purged from the stream
     * @return the count
     */
    @Deprecated
    public int getPurgedCount() {
        return purged.value().intValueExact();
    }

    /**
     * Returns the number of items purged from the stream
     * @return the count
     */
    public Ulong getPurged() {
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
