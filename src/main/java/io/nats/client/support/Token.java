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

    @Nullable
    public String getValueOrNull() {
        return hasValue ? valueAsString() : null;
    }

    private String valueAsString() {
        return new String(serialized, start, valueLength, US_ASCII).trim();
    }

    @NonNull
    public String getValueCheckKnownKeys() {
        if (valueLength == 0) {
            return EMPTY;
        }
        // all known keys are at least 5 characters Nats-<...> and KV-Operation
        if (valueLength > 5) {
            if (valueStartsNatsDash()) {
                if (endsMatch(NATS_STREAM_BYTES, 5)) {
                    return NATS_STREAM;
                }
                if (endsMatch(NATS_SEQUENCE_BYTES, 5)) {
                    return NATS_SEQUENCE;
                }
                if (endsMatch(NATS_TIMESTAMP_BYTES, 5)) {
                    return NATS_TIMESTAMP;
                }
                if (endsMatch(NATS_SUBJECT_BYTES, 5)) {
                    return NATS_SUBJECT;
                }
                if (endsMatch(NATS_LAST_SEQUENCE_BYTES, 5)) {
                    return NATS_LAST_SEQUENCE;
                }
                if (endsMatch(NATS_NUM_PENDING_BYTES, 5)) {
                    return NATS_NUM_PENDING;
                }
                if (endsMatch(CONSUMER_STALLED_HDR_BYTES, 5)) {
                    return CONSUMER_STALLED_HDR;
                }
                if (endsMatch(MSG_SIZE_HDR_BYTES, 5)) {
                    return MSG_SIZE_HDR;
                }
                if (endsMatch(NATS_MARKER_REASON_HDR_BYTES, 5)) {
                    return NATS_MARKER_REASON_HDR;
                }
                if (endsMatch(NATS_PENDING_MESSAGES_BYTES, 5)) {
                    return NATS_PENDING_MESSAGES;
                }
                if (endsMatch(NATS_PENDING_BYTES_BYTES, 5)) {
                    return NATS_PENDING_BYTES;
                }
            }
            else if (endsMatch(KV_OPERATION_HEADER_KEY_BYTES, 0)) {
                return KV_OPERATION_HEADER_KEY;
            }
        }

        // didn't know the key
        return valueAsString();
    }

    @Nullable
    public String getValueCheckKnownStatuses() {
        if (valueLength == 0) {
            return null;
        }
        if (valueLength > 11) {
            switch (serialized[start]) {
                case 'E':
                    if (valueStartsWithExceededMax()) {
                        if (endsMatch(EXCEEDED_MAX_WAITING_BYTES, 8)) {
                            return EXCEEDED_MAX_WAITING;
                        }
                        if (endsMatch(EXCEEDED_MAX_REQUEST_BATCH_BYTES, 8)) {
                            return EXCEEDED_MAX_REQUEST_BATCH;
                        }
                        if (endsMatch(EXCEEDED_MAX_REQUEST_EXPIRES_BYTES, 8)) {
                            return EXCEEDED_MAX_REQUEST_EXPIRES;
                        }
                        if (endsMatch(EXCEEDED_MAX_REQUEST_MAX_BYTES_BYTES, 8)) {
                            return EXCEEDED_MAX_REQUEST_MAX_BYTES;
                        }
                    }
                    break;
                case 'B':
                    if (endsMatch(BATCH_COMPLETED_BYTES, 1)) {
                        return BATCH_COMPLETED;
                    }
                    if (endsMatch(BAD_REQUEST_BYTES, 1)) {
                        return BAD_REQUEST;
                    }
                    break;
                case 'N':
                    if (endsMatch(NO_RESPONDERS_TEXT_BYTES, 1)) {
                        return NO_RESPONDERS_TEXT;
                    }
                    if (endsMatch(NO_MESSAGES_BYTES, 1)) {
                        return NO_MESSAGES;
                    }
                    break;
                case 'F':
                    if (endsMatch(FLOW_CONTROL_TEXT_BYTES, 1)) {
                        return FLOW_CONTROL_TEXT;
                    }
                    break;
                case 'I':
                    if (endsMatch(HEARTBEAT_TEXT_BYTES, 1)) {
                        return HEARTBEAT_TEXT;
                    }
                    break;
                case 'M':
                    if (endsMatch(MESSAGE_SIZE_EXCEEDS_MAX_BYTES_BYTES, 1)) {
                        return MESSAGE_SIZE_EXCEEDS_MAX_BYTES;
                    }
                    break;
                case 'L':
                    if (endsMatch(LEADERSHIP_CHANGE_BYTES, 1)) {
                        return LEADERSHIP_CHANGE;
                    }
                    break;
                case 'S':
                    if (endsMatch(SERVER_SHUTDOWN_BYTES, 1)) {
                        return SERVER_SHUTDOWN;
                    }
                    break;
                case 'C':
                    if (endsMatch(CONSUMER_DELETED_BYTES, 1)) {
                        return CONSUMER_DELETED;
                    }
                    if (endsMatch(CONSUMER_IS_PUSH_BASED_BYTES, 1)) {
                        return CONSUMER_IS_PUSH_BASED;
                    }
                    break;
            }
        }
        else if (endsMatch(EOB_TEXT_BYTES, 0)) { // only short status
            return EOB_TEXT;
        }
        return valueAsString();
    }

    static final byte[] NATS_DASH_PREFIX_BYTES = "Nats-".getBytes();
    static final byte[] EXCEEDED_MAX_PREFIX_BYTES = EXCEEDED_MAX_PREFIX.getBytes();
    static final int EXCEEDED_MAX_PREFIX_BYTES_LEN = EXCEEDED_MAX_PREFIX.length();

    private boolean valueStartsNatsDash() {
        for (int i = 0; i < 4; i++) {
            if (NATS_DASH_PREFIX_BYTES[i] != serialized[start + i]) {
                return false;
            }
        }
        return true;
    }

    private boolean valueStartsWithExceededMax() {
        // we know we already checked the first letter to be E
        for (int i = 1; i < EXCEEDED_MAX_PREFIX_BYTES_LEN; i++) {
            if (EXCEEDED_MAX_PREFIX_BYTES[i] != serialized[start + i]) {
                return false;
            }
        }
        return true;
    }


    private boolean endsMatch(byte @NonNull [] checkBytes, int compareStartIndex) {
        if (valueLength != checkBytes.length) {
            return false;
        }
        for (int i = compareStartIndex; i < valueLength; i++) {
            if (checkBytes[i] != serialized[start + i]) {
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
