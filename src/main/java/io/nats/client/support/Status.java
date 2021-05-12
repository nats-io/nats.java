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

    public static final String FLOW_CONTROL_TEXT = "FlowControl Request";
    public static final String HEARTBEAT_TEXT = "Idle Heartbeat";
    public static final String NO_RESPONDERS_TEXT = "No Responders Available For Request";
    public static final int FLOW_OR_HEARTBEAT_STATUS_CODE = 100;
    public static final int NO_RESPONDERS_CODE = 503;

    private final int code;
    private final String message;

    private static final Map<Integer, String> MESSAGE_MAP;

    static {
        MESSAGE_MAP = new HashMap<>();
        MESSAGE_MAP.put(NO_RESPONDERS_CODE, NO_RESPONDERS_TEXT);
    }

    public Status(int code, String message) {
        this.code = code;
        this.message = message == null ? makeMessage(code) : message ;
    }

    public Status(Token codeToken, Token messageToken) {
        this(extractCode(codeToken), extractMessage(messageToken));
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    private static String extractMessage(Token messageToken) {
        return messageToken.hasValue() ? messageToken.getValue() : null;
    }

    private static int extractCode(Token codeToken) {
        try {
            return Integer.parseInt(codeToken.getValue());
        }
        catch (Exception e) {
            throw new IllegalArgumentException(NatsConstants.INVALID_HEADER_STATUS_CODE);
        }
    }

    private String makeMessage(int code) {
        String message = MESSAGE_MAP.get(code);
        return message == null ? "Server Status Message: " + code : message;
    }

    @Override
    public String toString() {
        return "Status{" +
                "code=" + code +
                ", message='" + message + '\'' +
                '}';
    }

    private boolean isStatus(int code, String text) {
        return this.code == code && message.equals(text);
    }

    public boolean isFlowControl() {
        return isStatus(FLOW_OR_HEARTBEAT_STATUS_CODE, FLOW_CONTROL_TEXT);
    }

    public boolean isHeartbeat() {
        return isStatus(FLOW_OR_HEARTBEAT_STATUS_CODE, HEARTBEAT_TEXT);
    }

    public boolean isNoResponders() {
        return isStatus(NO_RESPONDERS_CODE, NO_RESPONDERS_TEXT);
    }
}
