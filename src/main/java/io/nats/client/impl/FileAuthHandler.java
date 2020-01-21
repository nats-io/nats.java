// Copyright 2018 The NATS Authors
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import io.nats.client.AuthHandler;
import io.nats.client.NKey;

class FileAuthHandler implements AuthHandler {
    private String jwtFile;
    private String nkeyFile;
    private String credsFile;

    FileAuthHandler(String creds) {
        this.credsFile = creds;
    }

    FileAuthHandler(String jwtFile, String nkeyFile) {
        this.jwtFile = jwtFile;
        this.nkeyFile = nkeyFile;
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
            if (!skipLine && headerCount==headers) {
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

    private char[] readKeyChars() throws IOException {
        char[] keyChars = null;

        if (this.credsFile != null) {
            byte[] data = Files.readAllBytes(Paths.get(this.credsFile));
            ByteBuffer bb = ByteBuffer.wrap(data);
            CharBuffer chars = StandardCharsets.UTF_8.decode(bb);
            keyChars = this.extract(chars, 3); // we are 2nd so 3 headers
            // Clear things up as best we can
            chars.clear();
            for (int i=0; i<chars.capacity();i++) {
                chars.put('\0');
            }
            for (int i=0;i<data.length;i++) {
                data[i] = 0;
            }
        } else {
            byte[] data = Files.readAllBytes(Paths.get(this.nkeyFile));
            ByteBuffer bb = ByteBuffer.wrap(data);
            CharBuffer chars = StandardCharsets.UTF_8.decode(bb);
            keyChars = this.extract(chars, 1);
            // Clear things up as best we can
            chars.clear();
            for (int i=0; i<chars.capacity();i++) {
                chars.put('\0');
            }
            for (int i=0;i<data.length;i++) {
                data[i] = 0;
            }
        }

        return keyChars;
    }

    /**
     * Sign is called by the library when the server sends a nonce.
     * The client's NKey should be used to sign the provided value.
     * 
     * @param nonce the nonce to sign
     * @return the signature for the nonce
     */ 
    public byte[] sign(byte[] nonce) {
        try {
            char[] keyChars = this.readKeyChars();
            NKey nkey =  NKey.fromSeed(keyChars);
            byte[] sig = nkey.sign(nonce);
            nkey.clear();
            return sig;
        } catch (Exception exp) {
            throw new IllegalStateException("problem signing nonce", exp);
        }
    }

    /**
     * getID should return a public key associated with a client key known to the server.
     * If the server is not in nonce-mode, this array can be empty.
     * 
     * @return the public key as a char array
     */
    public char[] getID() {
        try {
            char[] keyChars = this.readKeyChars();
            NKey nkey =  NKey.fromSeed(keyChars);
            char[] pubKey = nkey.getPublicKey();
            nkey.clear();
            return pubKey;
        } catch (Exception exp) {
            throw new IllegalStateException("problem getting public key", exp);
        }
    }

    /**
     * getJWT should return the user JWT associated with this connection.
     * This can return null for challenge only authentication, but for account/user
     * JWT-based authentication you need to return the JWT bytes here.
     * 
     * @return the user JWT
     */ 
    public char[] getJWT() {
        try {
            char[] jwtChars = null;
            String fileToUse = this.jwtFile;

            if (this.credsFile != null) {
                fileToUse = this.credsFile;
            }

            // If no file is provided, assume this is challenge only authentication
            // and simply return null here.
            if (fileToUse == null) {
                return null;
            }

            byte[] data = Files.readAllBytes(Paths.get(fileToUse));
            ByteBuffer bb = ByteBuffer.wrap(data);
            CharBuffer chars = StandardCharsets.UTF_8.decode(bb);
            jwtChars = this.extract(chars, 1); // jwt is always first
            // Clear things up as best we can
            chars.clear();
            for (int i=0; i<chars.capacity();i++) {
                chars.put('\0');
            }
            bb.clear();
            for (int i=0;i<data.length;i++) {
                data[i] = 0;
            }

            return jwtChars;
        } catch (Exception exp) {
            throw new IllegalStateException("problem reading jwt", exp);
        }
    }
}