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

public class IncomingHeadersProcessor {

    private final int serializedLength;
    private Headers headers;
    private Status inlineStatus;

    public IncomingHeadersProcessor(byte[] serialized) {
        this((serialized != null) ? ByteBuffer.wrap(serialized) : null);
    }

    public IncomingHeadersProcessor(ByteBuffer serialized) {
        // basic validation first to help fail fast
        if (serialized == null || serialized.limit() == 0) {
            throw new IllegalArgumentException(SERIALIZED_HEADER_CANNOT_BE_NULL_OR_EMPTY);
        }

        if (serialized.limit() < VERSION_BYTES_LEN + CRLF_BYTES_LEN) {
            throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
        }

        // is tis the correct version
        ByteBuffer version = serialized.duplicate();
        version.limit(VERSION_BYTES_LEN);
        if (!version.equals(VERSION)) {
            throw new IllegalArgumentException(INVALID_HEADER_VERSION);
        }

        // does the header end properly
        serializedLength = serialized.limit();
        new Token(serialized, serializedLength, serializedLength - 2, TokenType.CRLF);
        Token token = new Token(serialized, serializedLength, VERSION_BYTES_LEN, null);

        if (token.isType(TokenType.SPACE)) {
            initStatus(serialized, serializedLength, token);
        }
        else if (token.isType(TokenType.CRLF)) {
            initHeader(serialized, serializedLength, token);
        }
        else {
            throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
        }
    }

    public int getSerializedLength() {
        return serializedLength;
    }

    public Headers getHeaders() {
        return headers;
    }

    public Status getStatus() {
        return inlineStatus;
    }

    private void initHeader(ByteBuffer serialized, int len, Token tCrlf) {
        // REGULAR HEADER
        headers = new Headers();
        Token peek = new Token(serialized, len, tCrlf, null);
        while (peek.isType(TokenType.TEXT)) {
            Token tKey = new Token(serialized, len, tCrlf, TokenType.KEY);
            Token tVal = new Token(serialized, len, tKey, null);
            if (tVal.isType(TokenType.SPACE)) {
                tVal = new Token(serialized, len, tVal, null);
            }
            if (tVal.isType(TokenType.TEXT)) {
                tCrlf = new Token(serialized, len, tVal, TokenType.CRLF);
            }
            else {
                tVal.mustBe(TokenType.CRLF);
                tCrlf = tVal;
            }
            headers.add(tKey.getValue(), tVal.getValue());
            peek = new Token(serialized, len, tCrlf, null);
        }
        peek.mustBe(TokenType.CRLF);

        if (headers.size() == 0) {
            throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
        }
    }

    private void initStatus(ByteBuffer serialized, int len, Token tSpace) {
        Token tCode = new Token(serialized, len, tSpace, TokenType.WORD);
        Token tVal = new Token(serialized, len, tCode, null);
        if (tVal.isType(TokenType.SPACE)) {
            tVal = new Token(serialized, len, tVal, TokenType.TEXT);
            new Token(serialized, len, tVal, TokenType.CRLF);
        }
        else {
            tVal.mustBe(TokenType.CRLF);
        }
        inlineStatus = new Status(tCode, tVal);
    }

}
