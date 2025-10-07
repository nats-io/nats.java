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

import static io.nats.client.support.NatsConstants.*;

public class IncomingHeadersProcessor {

    private final int serializedLength;
    private Headers headers;
    private Status inlineStatus;

    public IncomingHeadersProcessor(byte[] serialized) {

        // basic validation first to help fail fast
        if (serialized == null || serialized.length == 0) {
            throw new IllegalArgumentException(SERIALIZED_HEADER_CANNOT_BE_NULL_OR_EMPTY);
        }

        // is this the correct version
        for (int x = 0; x < HEADER_VERSION_BYTES_LEN; x++) {
            if (serialized[x] != HEADER_VERSION_BYTES[x]) {
                throw new IllegalArgumentException(INVALID_HEADER_VERSION);
            }
        }

        // does the header end properly
        serializedLength = serialized.length;
        Token terminus = new Token(serialized, serializedLength, serializedLength - 2, TokenType.CRLF);
        Token token = new Token(serialized, serializedLength, HEADER_VERSION_BYTES_LEN, null);

        if (token.isType(TokenType.SPACE)) {
            token = initStatus(serialized, serializedLength, token);
            if (token.samePoint(terminus)) {
                return; // status only
            }
        }

        if (token.isType(TokenType.CRLF)) {
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

    private void initHeader(byte[] serialized, int len, Token tCrlf) {
        // REGULAR HEADER
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
            if (headers == null) {
                headers = new Headers();
            }
            headers.add(tKey.getValueCheckKnownKeys(), tVal.getValue());
            peek = new Token(serialized, len, tCrlf, null);
        }
        peek.mustBe(TokenType.CRLF);
    }

    private Token initStatus(byte[] serialized, int len, Token tSpace) {
        Token tCode = new Token(serialized, len, tSpace, TokenType.WORD);
        Token tVal = new Token(serialized, len, tCode, null);
        Token crlf;
        if (tVal.isType(TokenType.SPACE)) {
            tVal = new Token(serialized, len, tVal, TokenType.TEXT);
            crlf = new Token(serialized, len, tVal, TokenType.CRLF);
        }
        else {
            tVal.mustBe(TokenType.CRLF);
            crlf = tVal;
        }
        inlineStatus = new Status(tCode, tVal);
        return crlf;
    }

}
