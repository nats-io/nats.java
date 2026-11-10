// Copyright 2026 The NATS Authors
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

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import static io.nats.client.support.Validator.emptyAsNull;
import static io.nats.client.support.Validator.nullOrEmpty;
import static io.nats.service.ServiceMessage.NATS_SERVICE_ERROR;
import static io.nats.service.ServiceMessage.NATS_SERVICE_ERROR_CODE;

/**
 * A message received in reply to a service request, so the requester can tell
 * whether the service responded with an error. A reply is an error when it has a
 * {@value ServiceMessage#NATS_SERVICE_ERROR} or a {@value ServiceMessage#NATS_SERVICE_ERROR_CODE} header.
 */
public class ServiceReplyMessage {
    private final Message message;
    private final boolean error;
    private final String errorDescription;
    private final Integer errorCode;

    /**
     * Construct a ServiceReplyMessage from a reply message
     * @param message the reply message
     */
    public ServiceReplyMessage(@NonNull Message message) {
        this.message = message;
        Headers headers = message.getHeaders();
        if (headers == null) {
            error = false;
            errorDescription = null;
            errorCode = null;
        }
        else {
            errorDescription = emptyAsNull(headers.getFirst(NATS_SERVICE_ERROR));
            String sCode = emptyAsNull(headers.getFirst(NATS_SERVICE_ERROR_CODE));
            error = errorDescription != null || sCode != null;
            Integer code = null;
            if (sCode != null) {
                try {
                    code = Integer.parseInt(sCode);
                }
                catch (NumberFormatException ignore) {}
            }
            errorCode = code;
        }
    }

    /**
     * The subject of the message
     * @return the subject
     */
    @NonNull
    public String getSubject() {
        return message.getSubject();
    }

    /**
     * The headers of the message
     * @return the headers or null if the message has none
     */
    @Nullable
    public Headers getHeaders() {
        return message.getHeaders();
    }

    /**
     * The data of the message
     * @return the data, which is empty if the message has none
     */
    public byte @NonNull [] getData() {
        return message.getData();
    }

    /**
     * Whether the reply is a service error, meaning it has an error or error code header
     * @return true if the reply is a service error
     */
    public boolean isError() {
        return error;
    }

    /**
     * The value of the {@value ServiceMessage#NATS_SERVICE_ERROR} header
     * @return the error description or null if the reply has no such header
     */
    @Nullable
    public String getErrorDescription() {
        return errorDescription;
    }

    /**
     * The value of the {@value ServiceMessage#NATS_SERVICE_ERROR_CODE} header
     * @return the error code or null, if the error code was not set or invalid;
     */
    @Nullable
    public Integer getErrorCode() {
        return errorCode;
    }
}
