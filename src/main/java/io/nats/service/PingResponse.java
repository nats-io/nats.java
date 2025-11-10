// Copyright 2023 The NATS Authors
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

package io.nats.service;

import java.util.Map;

/**
 * Ping response class forms the ping json payload, for example:
 * <code>{"id":"JlkwZvmHAXCQGwwxiPwaBJ","name":"MyService","version":"0.0.1","type":"io.nats.micro.v1.ping_response"}</code>
 */
public class PingResponse extends ServiceResponse {
    /**
     * The API response type for PingResponse
     */
    public static final String TYPE = "io.nats.micro.v1.ping_response";

    PingResponse(String id, String name, String version, Map<String, String> metadata) {
        super(TYPE, id, name, version, metadata);
    }

    PingResponse(byte[] jsonBytes) {
        super(TYPE, parseMessage(jsonBytes));
    }
}
