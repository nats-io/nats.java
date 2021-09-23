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
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

public class JwtUtils {
    static class Nats implements JsonSerializable {
        String issuerAccount;
        String[] tags;

        @Override
        public String toJson() {
            StringBuilder sb = beginJson();
            JsonUtils.addField(sb, "issuer_account", issuerAccount);
            JsonUtils.addStrings(sb, "tags", Arrays.asList(tags));
            JsonUtils.addField(sb, "type", "user");
            JsonUtils.addField(sb, "version", 2);
            return endJson(sb).toString();
        }
    }

    static class Claim implements JsonSerializable {
        Duration exp;
        long iat;
        String iss;
        String jti;
        String name;
        Nats nats;
        String sub;

        @Override
        public String toJson() {
            StringBuilder sb = beginJson();
            if (exp != null && !exp.isZero() && !exp.isNegative()) {
                long unixSeconds = exp.toMillis() / 1000;
                JsonUtils.addField(sb, "exp", unixSeconds);
            }
            JsonUtils.addField(sb, "iat", iat);
            JsonUtils.addFieldEvenEmpty(sb, "jti", jti);
            JsonUtils.addField(sb, "iss", iss);
            JsonUtils.addField(sb, "name", name);
            JsonUtils.addField(sb, "nats", nats);
            JsonUtils.addField(sb, "sub", sub);

            return endJson(sb).toString();
        }
    }

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

    public static String issueUserCreds(String accSeed, String accId, String userSeed, String userKeyPub, String optionalName, Duration optionalExpiration, String... optionalTags) throws GeneralSecurityException, IOException {
        String jwt = issueUserJWT(accSeed, accId, userKeyPub, optionalName, optionalExpiration, optionalTags);
        return String.format(NATS_USER_JWT_FORMAT, jwt, userSeed);
    }

    public static String issueUserCreds(NKey accountSigningKey, String accId, String userSeed, String userKeyPub, String optionalName, Duration optionalExpiration, String... optionalTags) throws GeneralSecurityException, IOException {
        String jwt = issueUserJWT(accountSigningKey, accId, userKeyPub, optionalName, optionalExpiration, optionalTags);
        return String.format(NATS_USER_JWT_FORMAT, jwt, userSeed);
    }

    public static String issueUserJWT(String accSeed, String accId, String userKeyPub, String optionalName, Duration optionalExpiration, String... optionalTags) throws GeneralSecurityException, IOException {
        NKey accountSigningKey = NKey.fromSeed(accSeed.toCharArray());
        return issueUserJWT(accountSigningKey, accId, userKeyPub, optionalName, optionalExpiration, optionalTags);
    }

    public static String issueUserJWT(NKey accountSigningKey, String accId, String userKeyPub, String optionalName, Duration optionalExpiration, String... optionalTags) throws GeneralSecurityException, IOException {
        String accSigningKeyPub = new String(accountSigningKey.getPublicKey());

        Claim claim = new Claim();
        claim.exp = optionalExpiration;
        claim.iat = System.currentTimeMillis() / 1000;
        claim.iss = accSigningKeyPub;
        claim.name = Validator.nullOrEmpty(optionalName) ? userKeyPub : optionalName;
        claim.sub = userKeyPub;
        claim.nats = new Nats();
        claim.nats.issuerAccount = accId;
        claim.nats.tags = optionalTags;

        // Issue At time is stored in unix seconds
        String claimJson = claim.toJson();

        // Compute jti, a base32 encoded sha256 hash
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] encoded = sha256.digest(claimJson.getBytes(StandardCharsets.UTF_8));

        claim.jti = new String(NKey.base32Encode(encoded));
        claimJson = claim.toJson();

        // all three components (header/body/signature) are base64url encoded
        String encBody = ToBase64Url(claimJson);

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