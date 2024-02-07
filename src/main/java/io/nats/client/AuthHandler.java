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

package io.nats.client;

/**
 * NATS provides a challenge-response based authentication scheme based on {@link NKey NKeys}. Since 
 * NKeys depend on a private seed, we do not handle them directly in the client library. Instead, you can
 * work with them inside an AuthHandler that only makes the public key available to the library.
 * 
 * <pre>
 * {@code
    char[] nkey;
    char[] jwt;
    
    public byte[] sign(byte[] nonce) {
        try {
            NKey nkey =  NKey.fromSeed(this.nkey);
            byte[] sig = nkey.sign(nonce);
            nkey.clear();
            return sig;
        } catch (Exception exp) {
            throw new IllegalStateException("problem signing nonce", exp);
        }
    }

    public char[] getID() {
        try {
            NKey nkey =  NKey.fromSeed(this.nkey);
            char[] pubKey = nkey.getPublicKey();
            nkey.clear();
            return pubKey;
        } catch (Exception exp) {
            throw new IllegalStateException("problem getting public key", exp);
        }
    }

    public char[] getJWT() {
        return this.jwt;
    }
}
 * </pre>
 */
public interface AuthHandler {
    /**
     * Sign is called by the library when the server sends a nonce.
     * The client's NKey should be used to sign the provided value.
     * 
     * @param nonce the nonce to sign
     * @return the signature for the nonce
     */ 
    public byte[] sign(byte[] nonce);

    /**
     * getID should return a public key associated with a client key known to the server.
     * If the server is not in nonce-mode, this array can be empty.
     * 
     * @return the public key as a char array
     */
    public char[] getID();

    /**
     * getJWT should return the user JWT associated with this connection.
     * This can return null for challenge only authentication, but for account/user
     * JWT-based authentication you need to return the JWT bytes here.
     * 
     * @return the user JWT
     */ 
    public char[] getJWT();
}