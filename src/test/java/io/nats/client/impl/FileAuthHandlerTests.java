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

import static org.junit.Assert.assertArrayEquals;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import io.nats.client.AuthHandler;
import io.nats.client.NKey;
import io.nats.client.Nats;

public class FileAuthHandlerTests {

    private final static String JWT = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJTNkFFM0VMWUdCQzVNV0lRUVBFU05BWE8yNzROTDU1RFZFVDVaVDVLWlRRQkRVRVJPUUtBIiwiaWF0IjoxNTQzNjA1OTgxLCJpc3MiOiJBQVlXUTVWUkRUMjJGTVpKT0Y2MlZKSFpLNVBQUldXSlJDTklVVkhGVEQ0WkRMUFBaRkRXUlFSUiIsIm5hbWUiOiJmcmVlX3VzZXIiLCJzdWIiOiJVQVdERDVIRzM1N0tNQklLUjYzSExNNzJLVFFVVVNYWUVJRk1DVUFVQUVRRFRISVg3R1lHRVVWWSIsInR5cGUiOiJ1c2VyIiwibmF0cyI6eyJwdWIiOnt9LCJzdWIiOnt9fX0.HEJI1ADBMbPHRQT5FJFUGDKxd77jMVcA7MictlswfHyepWtifGGRcAkzhsT-EF182ifCz0f3t_9Qy-PEI2PRBQ";
    private final static String SEED = "SUAGVDADILKKEQSTWF7RTC25D3F433K3VWMQOGNJRE2VJGEP3LSSO7PHUE";
    
    @Test
    public void testCredsFile() throws Exception {
        AuthHandler auth = Nats.credentials("src/test/resources/jwt_nkey/test.creds");
        NKey key = NKey.fromSeed(SEED.toCharArray());
        byte[] test = "hello world".getBytes(StandardCharsets.UTF_8);

        char[] pubKey = auth.getID();
        assertArrayEquals(key.getPublicKey(), pubKey);
        assertArrayEquals(key.sign(test), auth.sign(test));
        assertArrayEquals(JWT.toCharArray(), auth.getJWT());
    }

    @Test
    public void testSeparateWrappedFiles() throws Exception {
        AuthHandler auth = Nats.credentials("src/test/resources/jwt_nkey/test_wrapped.jwt", "src/test/resources/jwt_nkey/test_wrapped.nk");
        NKey key = NKey.fromSeed(SEED.toCharArray());
        byte[] test = "hello world again".getBytes(StandardCharsets.UTF_8);

        char[] pubKey = auth.getID();
        assertArrayEquals(key.getPublicKey(), pubKey);
        assertArrayEquals(key.sign(test), auth.sign(test));
        assertArrayEquals(JWT.toCharArray(), auth.getJWT());
    }

    @Test
    public void testSeparateBareFiles() throws Exception {
        AuthHandler auth = Nats.credentials("src/test/resources/jwt_nkey/test.jwt", "src/test/resources/jwt_nkey/test.nk");
        NKey key = NKey.fromSeed(SEED.toCharArray());
        byte[] test = "hello world and again".getBytes(StandardCharsets.UTF_8);

        char[] pubKey = auth.getID();
        assertArrayEquals(key.getPublicKey(), pubKey);
        assertArrayEquals(key.sign(test), auth.sign(test));
        assertArrayEquals(JWT.toCharArray(), auth.getJWT());
    }
}