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

/**
 * JetStreamApiException is used to indicate that the server returned an error while make a request
 * related to JetStream.
 */
public class JetStreamApiException extends Exception {
    private final ApiResponse<?> apiResponse;

    /**
     * Construct an exception with the response from the server.
     *
     * @param apiResponse the response from the server.
     */
    public JetStreamApiException(ApiResponse<?> apiResponse) {
        super(apiResponse.getError());
        this.apiResponse = apiResponse;
    }

    /**
     * Get the error code from the response
     *
     * @return the code
     */
    public long getErrorCode() {
        return apiResponse.getErrorCode();
    }

    /**
     * Get the description from the response
     *
     * @return the description
     */
    public String getErrorDescription() {
        return apiResponse.getDescription();
    }
}
