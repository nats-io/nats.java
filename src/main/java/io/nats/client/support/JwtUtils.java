// Copyright 2015-2018 The NATS Authors
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

import io.nats.client.NKey;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;

public class JwtUtils {
    static final String CLAIM_FORMAT = "{{" +
            "\"iat\": \"%s\"," +
            "\"iss\": \"%s\"," +
            "\"jti\": \"%s\"," +
            "\"name\": \"%s\"," +
            "\"nats\": {{" +
            "\"data\": -1," +
            "\"issuer_account\": \"%s\"," +
            "\"payload\": -1," +
            "\"pub\": {{}}," +
            "\"sub\": {{}}," +
            "\"subs\": -1," +
            "\"type\": \"user\"," +
            "\"version\": 2" +
            "}}," +
            "\"sub\": \"%s\"" +
            "}}";

    static final String ENCODED_CLAIM_HEADER =
            ToBase64Url("{\"typ\":\"JWT\", \"alg\":\"ed25519-nkey\"}");

    static final String NATS_USER_JWT_FORMAT = "-----BEGIN NATS USER JWT-----\n" +
            "%s\n" +
            "------END NATS USER JWT------\n" +
            "\n" +
            "************************* IMPORTANT *************************\n" +
            "    NKEY Seed printed below can be used to sign and prove identity.\n" +
            "    NKEYs are sensitive and should be treated as secrets.\n" +
            "\n" +
            "-----BEGIN USER NKEY SEED-----\n" +
            "%s\n" +
            "------END USER NKEY SEED------\n" +
            "\n" +
            "*************************************************************";

    public static String issueUserCreds(String accSeed, String accId, String userSeed, String userKeyPub) throws GeneralSecurityException, IOException {
        String jwt = issueUserJWT(accSeed, accId, userKeyPub);
        return String.format(NATS_USER_JWT_FORMAT, jwt, userSeed);
    }

    public static String issueUserJWT(String accSeed, String accId, String userKeyPub) throws GeneralSecurityException, IOException {
        NKey accountSigningKey = NKey.fromSeed(accSeed.toCharArray());
        String accSigningKeyPub = new String(accountSigningKey.getPublicKey());

        // Issue At time is stored in unix seconds
        long issuedAt = System.currentTimeMillis() / 1000;
        String claim = String.format(CLAIM_FORMAT,
                issuedAt,
                accSigningKeyPub,
                "", /* blank jti */
                userKeyPub,
                accId,
                userKeyPub);

        // Compute jti, a base32 encoded sha256 hash
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] encoded = sha256.digest(claim.getBytes(StandardCharsets.UTF_8));
        String jti = new String(NKey.base32Encode(encoded));

        claim = String.format(CLAIM_FORMAT,
                issuedAt,
                accSigningKeyPub,
                jti,
                userKeyPub,
                accId,
                userKeyPub);

        // all three components (header/body/signature) are base64url encoded
        String encBody = ToBase64Url(claim);

        // compute the signature off of header + body (. included on purpose)
        byte[] sig = (ENCODED_CLAIM_HEADER + "." + encBody).getBytes(StandardCharsets.UTF_8);
        String encSig = ToBase64Url(accountSigningKey.sign(sig));

        // append signature to header and body and return it
        return ENCODED_CLAIM_HEADER + "." + encBody + "." + encSig;
    }

    public static String ToBase64Url(byte[] input) {
        return new String(Base64.getUrlEncoder().encode(input));
    }

    public static String ToBase64Url(String input) {
        return ToBase64Url(input.getBytes(StandardCharsets.UTF_8));
    }
}