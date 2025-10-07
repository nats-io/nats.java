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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import static io.nats.client.support.NatsConstants.*;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.Status.*;
import static java.nio.charset.StandardCharsets.US_ASCII;

public class Token {
    private final byte[] serialized;
    private final TokenType type;
    private final int start;
    private int end;
    private boolean hasValue;
    private final int valueLength;

    public Token(byte[] serialized, int len, Token prev, TokenType required) {
        this(serialized, len, prev.end + (prev.type == TokenType.KEY ? 2 : 1), required);
    }

    public Token(byte[] serialized, int len, int cur, TokenType required) {
        this.serialized = serialized;

        if (cur >= len) {
            throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
        }
        if (serialized[cur] == SP) {
            type = TokenType.SPACE;
            start = cur;
            end = cur;
            while (serialized[++cur] == SP) {
                end = cur;
            }
        } else if (serialized[cur] == CR) {
            mustBeCrlf(len, cur);
            type = TokenType.CRLF;
            start = cur;
            end = cur + 1;
        } else if (required == TokenType.CRLF || required == TokenType.SPACE) {
            throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
        } else {
            byte ender1;
            byte ender2;
            if (required == null || required == TokenType.TEXT) {
                type = TokenType.TEXT;
                ender1 = CR;
                ender2 = CR;
            }
            else if (required == TokenType.WORD) {
                ender1 = SP;
                ender2 = CR;
                type = TokenType.WORD;
            } else { // KEY is all that's left if (required == TokenType.KEY) {
                ender1 = COLON;
                ender2 = COLON;
                type = TokenType.KEY;
            }
            start = cur;
            end = cur;
            while (++cur < len && serialized[cur] != ender1 && serialized[cur] != ender2) {
                end = cur;
            }
            if (cur >= len) {
                throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
            }
            if (serialized[cur] == CR) {
                mustBeCrlf(len, cur);
            }
            hasValue = true;
        }
        valueLength = hasValue ? end - start + 1 : 0;
    }

    private void mustBeCrlf(int len, int cur) {
        if ((cur + 1) >= len || serialized[cur + 1] != LF) {
            throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
        }
    }

    public void mustBe(TokenType expected) {
        if (type != expected) {
            throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
        }
    }

    public boolean isType(TokenType expected) {
        return type == expected;
    }

    public boolean hasValue() {
        return hasValue;
    }

    @NonNull
    public String getValue() {
        return hasValue ? valueAsString() : EMPTY;
    }

    private String valueAsString() {
        return new String(serialized, start, valueLength, US_ASCII).trim();
    }

    @NonNull
    public String getValueCheckKnownKeys() {
        if (valueLength == 0) {
            return EMPTY;
        }
        byte b = serialized[start];
        if (b == 'N') {
            if (valueEquals(NATS_STREAM_BYTES)) {
                return NATS_STREAM;
            }
            if (valueEquals(NATS_SEQUENCE_BYTES)) {
                return NATS_SEQUENCE;
            }
            if (valueEquals(NATS_TIMESTAMP_BYTES)) {
                return NATS_TIMESTAMP;
            }
            if (valueEquals(NATS_SUBJECT_BYTES)) {
                return NATS_SUBJECT;
            }
            if (valueEquals(NATS_LAST_SEQUENCE_BYTES)) {
                return NATS_LAST_SEQUENCE;
            }
            if (valueEquals(NATS_NUM_PENDING_BYTES)) {
                return NATS_NUM_PENDING;
            }
        }
        return valueAsString();
    }

    @Nullable
    public String getValueCheckKnownStatuses() {
        if (valueLength == 0) {
            return null;
        }
        byte b = serialized[start];
        if (b == 'B') {
            if (valueEquals(BATCH_COMPLETED_BYTES)) {
                return BATCH_COMPLETED;
            }
            if (valueEquals(BAD_REQUEST_BYTES)) {
                return BAD_REQUEST;
            }
        }
        else if (b == 'E') {
            if (valueEquals(EXCEEDED_MAX_PREFIX_BYTES)) {
                return EXCEEDED_MAX_PREFIX;
            }
            if (valueEquals(EXCEEDED_MAX_WAITING_BYTES)) {
                return EXCEEDED_MAX_WAITING;
            }
            if (valueEquals(EXCEEDED_MAX_REQUEST_BATCH_BYTES)) {
                return EXCEEDED_MAX_REQUEST_BATCH;
            }
            if (valueEquals(EXCEEDED_MAX_REQUEST_EXPIRES_BYTES)) {
                return EXCEEDED_MAX_REQUEST_EXPIRES;
            }
            if (valueEquals(EXCEEDED_MAX_REQUEST_MAX_BYTES_BYTES)) {
                return EXCEEDED_MAX_REQUEST_MAX_BYTES;
            }
            if (valueEquals(EOB_TEXT_BYTES)) {
                return EOB_TEXT;
            }
        }
        else if (b == 'N') {
            if (valueEquals(NO_RESPONDERS_TEXT_BYTES)) {
                return NO_RESPONDERS_TEXT;
            }
            if (valueEquals(NO_MESSAGES_BYTES)) {
                return NO_MESSAGES;
            }
        }
        else if (b == 'F') {
            if (valueEquals(FLOW_CONTROL_TEXT_BYTES)) {
                return FLOW_CONTROL_TEXT;
            }
        }
        else if (b == 'I') {
            if (valueEquals(HEARTBEAT_TEXT_BYTES)) {
                return HEARTBEAT_TEXT;
            }
        }
        else if (b == 'M') {
            if (valueEquals(MESSAGE_SIZE_EXCEEDS_MAX_BYTES_BYTES)) {
                return MESSAGE_SIZE_EXCEEDS_MAX_BYTES;
            }
        }
        else if (b == 'L') {
            if (valueEquals(LEADERSHIP_CHANGE_BYTES)) {
                return LEADERSHIP_CHANGE;
            }
        }
        else if (b == 'S') {
            if (valueEquals(SERVER_SHUTDOWN_BYTES)) {
                return SERVER_SHUTDOWN;
            }
        }
        else if (b == 'C') {
            if (valueEquals(CONSUMER_DELETED_BYTES)) {
                return CONSUMER_DELETED;
            }
            if (valueEquals(CONSUMER_IS_PUSH_BASED_BYTES)) {
                return CONSUMER_IS_PUSH_BASED;
            }
        }
        return valueAsString();
    }

    public Integer getIntValue() throws NumberFormatException {
        if (valueLength == 0) {
            return null;
        }
        byte b = serialized[start];
        if (b == '4') {
            if (valueEquals(BAD_REQUEST_CODE_BYTES)) {
                return BAD_REQUEST_CODE;
            }
            if (valueEquals(NOT_FOUND_CODE_BYTES)) {
                return NOT_FOUND_CODE;
            }
            if (valueEquals(BAD_JS_REQUEST_CODE_BYTES)) {
                return BAD_JS_REQUEST_CODE;
            }
            if (valueEquals(CONFLICT_CODE_BYTES)) {
                return CONFLICT_CODE;
            }
        }
        else if (b == '1') {
            if (valueEquals(FLOW_OR_HEARTBEAT_STATUS_CODE_BYTES)) {
                return FLOW_OR_HEARTBEAT_STATUS_CODE;
            }
        }
        else if (b == '5') {
            if (valueEquals(NO_RESPONDERS_CODE_BYTES)) {
                return NO_RESPONDERS_CODE;
            }
        }
        else if (b == '2') {
            if (valueEquals(EOB_CODE_BYTES)) {
                return EOB_CODE;
            }
        }
        return Integer.parseInt(getValue());
    }

    public boolean valueEquals(byte @NonNull [] bytes) {
        if (valueLength != bytes.length) {
            return false;
        }

        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != serialized[i + start]) {
                return false;
            }
        }

        return true;
    }

    public boolean samePoint(Token token) {
        return start == token.start
                && end == token.end
                && type == token.type;
    }
}
