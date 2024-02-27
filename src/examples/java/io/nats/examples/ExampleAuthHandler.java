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

package io.nats.examples;

import io.nats.client.AuthHandler;
import io.nats.client.NKey;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;

public class ExampleAuthHandler implements AuthHandler {
    private final NKey nkey;

    public ExampleAuthHandler(String path) throws Exception {
        File f = new File(path);
        int numBytes = (int)f.length();
        try (BufferedReader in = new BufferedReader(new FileReader(f))) {
            char[] buffer = new char[numBytes];
            int numChars = in.read(buffer);
            if (numChars < numBytes) {
                char[] seed = new char[numChars];
                System.arraycopy(buffer, 0, seed, 0, numChars);
                this.nkey = NKey.fromSeed(seed);
                Arrays.fill(seed, '\0'); // clear memory
            }
            else {
                this.nkey = NKey.fromSeed(buffer);
            }
            Arrays.fill(buffer, '\0'); // clear memory
        }
    }

    public NKey getNKey() {
        return this.nkey;
    }

    public char[] getID() {
        try {
            return this.nkey.getPublicKey();
        } catch (GeneralSecurityException|IOException|NullPointerException ex) {
            return null;
        }
    }

    public byte[] sign(byte[] nonce) {
        try {
            return this.nkey.sign(nonce);
        } catch (GeneralSecurityException|IOException|NullPointerException ex) {
            return null;
        }
    }

    public char[] getJWT() {
        return null;
    }
}
