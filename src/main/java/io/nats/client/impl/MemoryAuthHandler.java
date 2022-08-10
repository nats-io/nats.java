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

package io.nats.client.impl;

import io.nats.client.AuthHandler;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

public class MemoryAuthHandler implements AuthHandler {

    private final StringAuthHandler sah;

    public MemoryAuthHandler(byte[] input) {
        ByteBuffer bb = ByteBuffer.wrap(input);
        CharBuffer chars = StandardCharsets.UTF_8.decode(bb);
        char[] jwt = this.extract(chars, 1);
        char[] nkey = this.extract(chars, 2);
        sah = new StringAuthHandler(jwt, nkey);
    }

    private char[] extract(CharBuffer data, int headers) {
        CharBuffer buff = CharBuffer.allocate(data.length());
        boolean skipLine = false;
        int headerCount = 0;
        int linePos = -1;

        while (data.length() > 0) {
            char c = data.get();
            linePos++;

            // End of line, either we got it, or we should keep reading the new line
            if (c == '\n' || c=='\r') {
                if (buff.position() > 0) { // we wrote something
                    break;
                }
                skipLine = false;
                linePos = -1; // so we can start right up
                continue;
            }

            // skip to the new line
            if (skipLine) {
                continue;
            }

            // Ignore whitespace
            if (Character.isWhitespace(c)) {
                continue;
            }

            // If we are on a - skip that line, bump the header count
            if (c == '-' && linePos == 0) {
                skipLine = true;
                headerCount++;
                continue;
            }

            // Skip the line, or add to buff
            if (headerCount==headers) {
                buff.put(c);
            }
        }

        // check for naked value
        if (buff.position() == 0 && headers==1) {
            data.position(0);
            while (data.length() > 0) {
                char c = data.get();
                if (c == '\n' || c=='\r' || Character.isWhitespace(c)) {
                    if (buff.position() > 0) { // we wrote something
                        break;
                    }
                    continue;
                }
    
                buff.put(c);
            }
            buff.flip();
        } else {
            buff.flip();
        }

        char[] retVal = new char[buff.length()];
        buff.get(retVal);
        buff.clear();
        for (int i=0; i<buff.capacity();i++) {
            buff.put('\0');
        }
        return retVal;
    }

    /**
     * Sign is called by the library when the server sends a nonce.
     * The client's NKey should be used to sign the provided value.
     * 
     * @param nonce the nonce to sign
     * @return the signature for the nonce
     */ 
    public byte[] sign(byte[] nonce) {
        return sah.sign(nonce);
    }

    /**
     * getID should return a public key associated with a client key known to the server.
     * If the server is not in nonce-mode, this array can be empty.
     * 
     * @return the public key as a char array
     */
    public char[] getID() {
        return sah.getID();
    }

    /**
     * getJWT should return the user JWT associated with this connection.
     * This can return null for challenge only authentication, but for account/user
     * JWT-based authentication you need to return the JWT bytes here.
     * 
     * @return the user JWT
     */ 
    public char[] getJWT() {
        return sah.getJWT();
    }
}