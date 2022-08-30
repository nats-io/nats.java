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
package io.nats.client.support;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * This is a utility class for making digesting data.
 */
public class Digester {
    public static final String DEFAULT_DIGEST_ALGORITHM = "SHA-256";
    public static final Charset DEFAULT_STRING_ENCODING = StandardCharsets.UTF_8;

    private final Charset stringCharset;
    private final Base64.Encoder encoder;
    private final MessageDigest digest;

    public Digester() throws NoSuchAlgorithmException {
        this(null, null, null);
    }

    public Digester(Base64.Encoder encoder) throws NoSuchAlgorithmException {
        this(null, null, encoder);
    }

    public Digester(String digestAlgorithm) throws NoSuchAlgorithmException {
        this(digestAlgorithm, null, null);
    }

    public Digester(String digestAlgorithm, Charset stringCharset, Base64.Encoder encoder) throws NoSuchAlgorithmException {
        this.stringCharset = stringCharset == null ? DEFAULT_STRING_ENCODING : stringCharset;
        this.encoder = encoder == null ? Base64.getUrlEncoder() : encoder;
        this.digest = MessageDigest.getInstance(
            digestAlgorithm == null ? DEFAULT_DIGEST_ALGORITHM : digestAlgorithm);
    }

    public Digester update(String input) {
        digest.update(input.getBytes(stringCharset));
        return this;
    }

    public Digester update(byte[] input) {
        digest.update(input);
        return this;
    }

    public Digester update(byte[] input, int offset, int len) {
        digest.update(input, offset, len);
        return this;
    }

    public Digester reset() {
        digest.reset();
        return this;
    }

    public Digester reset(String input) {
        return reset().update(input);
    }

    public Digester reset(byte[] input) {
        return reset().update(input);
    }

    public Digester reset(byte[] input, int offset, int len) {
        return reset().update(input, offset, len);
    }

    public String getDigestValue() {
        return encoder.encodeToString(digest.digest());
    }

    public String getDigestEntry() {
        return digest.getAlgorithm() + "=" + encoder.encodeToString(digest.digest());
    }

    public boolean matches(String digestEntry) {
        String algo = digest.getAlgorithm().toUpperCase();
        if (!digestEntry.toUpperCase().startsWith(algo)) {
            return false;
        }
        return getDigestValue().equals(digestEntry.substring(algo.length() + 1)); // + 1 for equals
    }
}
