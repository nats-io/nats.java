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

import io.nats.client.impl.Headers;

import java.nio.ByteBuffer;

import static io.nats.client.support.NatsConstants.*;
import static java.nio.charset.StandardCharsets.US_ASCII;

public class Token {
    private ByteBuffer serialized;
    private TokenType type;
    private int start;
    private int end;
    private boolean hasValue;

    public Token(ByteBuffer serialized, int len, Token prev, TokenType required) {
        this(serialized, len, prev.end + (prev.type == TokenType.KEY ? 2 : 1), required);
    }

    public Token(ByteBuffer serialized, int len, int cur, TokenType required) {
        this.serialized = serialized.duplicate();
        if (cur >= len) {
            throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
        }
        if (this.serialized.get(cur) == SP) {
            type = TokenType.SPACE;
            start = cur;
            end = cur;
            while (this.serialized.get(++cur) == SP) {
                end = cur;
            }
        }
        else if (this.serialized.get(cur) == CR) {
            mustBeCrlf(len, cur);
            type = TokenType.CRLF;
            start = cur;
            end = cur + 1;
        }
        else if (required == TokenType.CRLF || required == TokenType.SPACE) {
            mustBe(required);
        } else {
            byte ender1 = CR;
            byte ender2 = CR;
            if (required == null || required == TokenType.TEXT){
                type = TokenType.TEXT;
            }
            else if (required == TokenType.WORD){
                ender1 = SP;
                ender2 = CR;
                type = TokenType.WORD;
            }
            else if (required == TokenType.KEY){
                ender1 = COLON;
                ender2 = COLON;
                type = TokenType.KEY;
            }
            start = cur;
            end = cur;
            while (++cur < len && this.serialized.get(cur) != ender1 && this.serialized.get(cur) != ender2) {
                end = cur;
            }
            if (this.serialized.get(cur) == CR) {
                mustBeCrlf(len ,cur);
            }
            hasValue = true;
        }
        if (hasValue) {
            this.serialized.limit(end + 1);
            this.serialized.position(start);
        }
    }

    private void mustBeCrlf(int len, int cur) {
        if ((cur + 1) >= len || this.serialized.get(cur + 1) != LF) {
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

    public ByteBuffer getValue() {
        return hasValue ? this.serialized.slice() : EMPTY_BUFFER;
    }
}
