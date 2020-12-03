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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class Status {

    private final int code;
    private final ByteBuffer message;

    private static final Map<Integer, ByteBuffer> MESSAGE_MAP;

    static {
        MESSAGE_MAP = new HashMap<>();
        MESSAGE_MAP.put(503, US_ASCII.encode("No responders available for request"));
    }

    public Status(int code, ByteBuffer message) {
        this.code = code;
        this.message = message == null ? makeMessage(code) : message;
    }

    public Status(Token codeToken, Token messageToken) {
        this(extractCode(codeToken), extractMessage(messageToken));
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return US_ASCII.decode(message.asReadOnlyBuffer()).toString();
    }

    private static ByteBuffer extractMessage(Token messageToken) {
        return messageToken.hasValue() ? messageToken.getValue() : null;
    }

    private static int extractCode(Token codeToken) {
        try {
            return Integer.parseInt(US_ASCII.decode(codeToken.getValue().asReadOnlyBuffer()).toString());
        }
        catch (Exception e) {
            throw new IllegalArgumentException(NatsConstants.INVALID_HEADER_STATUS_CODE);
        }
    }

    private ByteBuffer makeMessage(int code) {
        ByteBuffer message = MESSAGE_MAP.get(code);
        return message == null ? US_ASCII.encode("Server Status Message: " + code) : message;
    }
}
