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

package io.nats.service.api;

import io.nats.client.impl.Headers;

import static io.nats.service.ServiceUtil.NATS_SERVICE_ERROR;
import static io.nats.service.ServiceUtil.NATS_SERVICE_ERROR_CODE;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class ServiceException extends RuntimeException {
    String message;
    int code;

    public ServiceException(String message) {
        this(message, -1);
    }

    public ServiceException(String message, int code) {
        super(message);
        this.message = message;
        this.code = code;
    }

    private static String exMessage(String message, int code) {
        return message + " [" + code + "]";
    }

    public Headers getHeaders() {
        return new Headers().put(NATS_SERVICE_ERROR, message).put(NATS_SERVICE_ERROR_CODE, "" + code);
    }

    public static ServiceException getInstance(Throwable t) {
        if (t instanceof ServiceException) {
            return (ServiceException)t;
        }
        return new ServiceException(t.getMessage(), 400);
    }
}
