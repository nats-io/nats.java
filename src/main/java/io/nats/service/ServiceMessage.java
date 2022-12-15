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

package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

import java.nio.charset.StandardCharsets;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public abstract class ServiceMessage {

    public static final String NATS_SERVICE_ERROR = "Nats-Service-Error";
    public static final String NATS_SERVICE_ERROR_CODE = "Nats-Service-Error-Code";

    public static void reply(Connection conn, Message request, byte[] data, Headers headers) {
        conn.publish(NatsMessage.builder()
            .subject(request.getReplyTo())
            .data(data)
            .headers(headers)
            .build());
    }

    public static void reply(Connection conn, Message request, String data) {
        reply(conn, request, data.getBytes(StandardCharsets.UTF_8), null);
    }

    public static void reply(Connection conn, Message request, String data, Headers headers) {
        reply(conn, request, data.getBytes(StandardCharsets.UTF_8), headers);
    }

    public static void replyStandardError(Connection conn, Message request, String errorMessage, int errorCode) {
        reply(conn, request, (byte[])null, new Headers()
            .put(NATS_SERVICE_ERROR, errorMessage)
            .put(NATS_SERVICE_ERROR_CODE, "" + errorCode));
    }
}
