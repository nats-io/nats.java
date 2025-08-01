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

package io.nats.client;

import io.nats.client.api.ApiResponse;
import io.nats.client.api.Error;
import org.jspecify.annotations.NonNull;

/**
 * JetStreamApiException is used to indicate that the server returned an error while make a request
 * related to JetStream.
 */
public class JetStreamApiException extends Exception {
    private final Error error;

    /**
     * @deprecated Prefer to construct with JetStreamApiException(@NonNull Error error)
     * Construct an exception with the response from the server.
     * @param apiResponse the response from the server.
     */
    @Deprecated
    public JetStreamApiException(ApiResponse<?> apiResponse) {
        // deprecated because of getErrorObject() is marked as @Nullable
        this(apiResponse.getErrorObject());
    }

    public JetStreamApiException(@NonNull Error error) {
        super(error.toString());
        this.error = error;
    }

    /**
     * Get the error code from the response
     *
     * @return the code
     */
    public int getErrorCode() {
        return error.getCode();
    }

    /**
     * Get the error code from the response
     *
     * @return the code
     */
    public int getApiErrorCode() {
        return error.getApiErrorCode();
    }

    /**
     * Get the description from the response
     *
     * @return the description
     */
    public String getErrorDescription() {
        return error.getDescription();
    }
}
