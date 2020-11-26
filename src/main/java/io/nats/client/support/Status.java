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

package io.nats.client.support;

import java.util.HashMap;
import java.util.Map;

public class Status {

    private final int code;
    private final String message;

    private static final Map<Integer, String> MESSAGE_MAP;

    static {
        MESSAGE_MAP = new HashMap<>();
        MESSAGE_MAP.put(503, "No responders available for request");
    }

    public Status(Token codeToken, Token messageToken) {
        try {
            code = Integer.parseInt(codeToken.getValue());
            if (messageToken.hasValue()) {
                message = messageToken.getValue();
            }
            else {
                message = MESSAGE_MAP.getOrDefault(code, "Server Status Message: " + code);
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException(NatsConstants.INVALID_HEADER_STATUS_CODE);
        }
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public boolean isException() { return true; } // trying to imagine future uses
}
