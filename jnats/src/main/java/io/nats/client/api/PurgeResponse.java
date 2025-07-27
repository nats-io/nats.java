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

import static io.nats.client.support.ApiConstants.PURGED;
import static io.nats.client.support.ApiConstants.SUCCESS;
import static io.nats.client.support.JsonValueUtils.readBoolean;
import static io.nats.client.support.JsonValueUtils.readLong;

public class PurgeResponse extends ApiResponse<PurgeResponse> {

    private final boolean success;
    private final long purged;

    public PurgeResponse(Message msg) {
        super(msg);
        success = readBoolean(jv, SUCCESS);
        purged = readLong(jv, PURGED, 0);
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
     * @deprecated
     * This method is replaced since the purged value is a long
     * value, not an int value
     * See {@link #getPurged()} instead.
     * @return the count
     */
    @Deprecated
    public int getPurgedCount() {
        return new Long(purged).intValue();
    }

    /**
     * Returns the number of items purged from the stream
     * @return the count
     */
    public long getPurged() {
        return purged;
    }
}
