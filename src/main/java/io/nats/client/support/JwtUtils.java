// Copyright 2021 The NATS Authors
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
import java.util.Collections;

import static io.nats.client.support.Encoding.base32Encode;
import static io.nats.client.support.Encoding.toBase64Url;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;

/**
 * Implements https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-14.md
 */
public class JwtUtils {
    static class Nats implements JsonSerializable {
        String issuerAccount;
        String[] tags;

        @Override
        public String toJson() {
            StringBuilder sb = beginJson();
            JsonUtils.addField(sb, "issuer_account", issuerAccount);
            JsonUtils.addStrings(sb, "tags", null == tags ? Collections.emptyList() : Arrays.asList(tags));
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
                long seconds = exp.toMillis() / 1000;
                JsonUtils.addField(sb, "exp", iat + seconds);
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

    private static final String ENCODED_CLAIM_HEADER =
            toBase64Url("{\"typ\":\"JWT\", \"alg\":\"ed25519-nkey\"}");

    /**
     * Format string with `%s` place holder for the JWT token followed
     * by the user NKey seed. This can be directly used as such:
     * 
     * <pre>
     * NKey userKey = NKey.createUser(new SecureRandom());
     * NKey signingKey = loadFromSecretStore();
     * String jwt = issueUserJWT(signingKey, accountId, new String(userKey.getPublicKey()));
     * String.format(JwtUtils.NATS_USER_JWT_FORMAT, jwt, new String(userKey.getSeed()));
     * </pre>
     */
    public static final String NATS_USER_JWT_FORMAT = "-----BEGIN NATS USER JWT-----\n" +
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
            "*************************************************************\n";

    /**
     * Issue a user JWT from a scoped signing key. See https://docs.nats.io/nats-tools/nsc/signing_keys
     * 
     * @param signingKey a mandatory account nkey pair to sign the generated jwt.
     * @param accountId a mandatory public account nkey. Will throw error when not set or not account nkey.
     * @param publicUserKey a mandatory public user nkey. Will throw error when not set or not user nkey.
     * @throws IllegalArgumentException if the accountId or publicUserKey is not a valid public key of the proper type
     * @throws NullPointerException if signingKey, accountId, or publicUserKey are null.
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException if signingKey sign method throws this exception.
     * @return a JWT
     */
    public static String issueUserJWT(NKey signingKey, String accountId, String publicUserKey) throws GeneralSecurityException, IOException {
        return issueUserJWT(signingKey, accountId, publicUserKey, null, null);
    }

    /**
     * Issue a user JWT from a scoped signing key. See https://docs.nats.io/nats-tools/nsc/signing_keys
     * 
     * @param signingKey a mandatory account nkey pair to sign the generated jwt.
     * @param accountId a mandatory public account nkey. Will throw error when not set or not account nkey.
     * @param publicUserKey a mandatory public user nkey. Will throw error when not set or not user nkey.
     * @param name optional human readable name. When absent, default to publicUserKey.
     * @throws IllegalArgumentException if the accountId or publicUserKey is not a valid public key of the proper type
     * @throws NullPointerException if signingKey, accountId, or publicUserKey are null.
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException if signingKey sign method throws this exception.
     * @return a JWT
     */
    public static String issueUserJWT(NKey signingKey, String accountId, String publicUserKey, String name) throws GeneralSecurityException, IOException {
        return issueUserJWT(signingKey, accountId, publicUserKey, name, null);
    }

    /**
     * Issue a user JWT from a scoped signing key. See https://docs.nats.io/nats-tools/nsc/signing_keys
     * 
     * @param signingKey a mandatory account nkey pair to sign the generated jwt.
     * @param accountId a mandatory public account nkey. Will throw error when not set or not account nkey.
     * @param publicUserKey a mandatory public user nkey. Will throw error when not set or not user nkey.
     * @param name optional human readable name. When absent, default to publicUserKey.
     * @param expiration optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.
     * @param tags optional list of tags to be included in the JWT.
     * @throws IllegalArgumentException if the accountId or publicUserKey is not a valid public key of the proper type
     * @throws NullPointerException if signingKey, accountId, or publicUserKey are null.
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException if signingKey sign method throws this exception.
     * @return a JWT
     */
    public static String issueUserJWT(NKey signingKey, String accountId, String publicUserKey, String name, Duration expiration, String... tags) throws GeneralSecurityException, IOException {
        return issueUserJWT(signingKey, accountId, publicUserKey, name, expiration, tags, System.currentTimeMillis() / 1000);
    }

    /**
     * Method used for testing.
     *
     * @param signingKey a mandatory account nkey pair to sign the generated jwt.
     * @param accountId a mandatory public account nkey. Will throw error when not set or not account nkey.
     * @param publicUserKey a mandatory public user nkey. Will throw error when not set or not user nkey.
     * @param name optional human readable name. When absent, default to publicUserKey.
     * @param expiration optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.
     * @param tags optional list of tags to be included in the JWT.
     * @param issuedAt the current epoch seconds.
     * @throws IllegalArgumentException if the accountId or publicUserKey is not a valid public key of the proper type
     * @throws NullPointerException if signingKey, accountId, or publicUserKey are null.
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException if signingKey sign method throws this exception.
     * @return a JWT
     */
    protected static String issueUserJWT(NKey signingKey, String accountId, String publicUserKey, String name, Duration expiration, String[] tags, long issuedAt) throws GeneralSecurityException, IOException {
        // Validate the signingKey:
        if (signingKey.getType() != NKey.Type.ACCOUNT) {
            throw new IllegalArgumentException("issueUserJWT requires an account key for the signingKey parameter, but got " + signingKey.getType());
        }
        // Validate the accountId:
        NKey accountKey = NKey.fromPublicKey(accountId.toCharArray());
        if (accountKey.getType() != NKey.Type.ACCOUNT) {
            throw new IllegalArgumentException("issueUserJWT requires an account key for the accountId parameter, but got " + accountKey.getType());
        }
        // Validate the publicUserKey:
        NKey userKey = NKey.fromPublicKey(publicUserKey.toCharArray());
        if (userKey.getType() != NKey.Type.USER) {
            throw new IllegalArgumentException("issueUserJWT requires a user key for the publicUserKey, but got " + userKey.getType());
        }
        String accSigningKeyPub = new String(signingKey.getPublicKey());

        Claim claim = new Claim();
        claim.exp = expiration;
        claim.iat = issuedAt;
        claim.iss = accSigningKeyPub;
        claim.name = Validator.nullOrEmpty(name) ? publicUserKey : name;
        claim.sub = publicUserKey;
        claim.nats = new Nats();
        claim.nats.issuerAccount = accountId;
        claim.nats.tags = tags;

        // Issue At time is stored in unix seconds
        String claimJson = claim.toJson();

        // Compute jti, a base32 encoded sha256 hash
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] encoded = sha256.digest(claimJson.getBytes(StandardCharsets.UTF_8));

        claim.jti = new String(base32Encode(encoded));
        claimJson = claim.toJson();

        // all three components (header/body/signature) are base64url encoded
        String encBody = toBase64Url(claimJson);

        // compute the signature off of header + body (. included on purpose)
        byte[] sig = (ENCODED_CLAIM_HEADER + "." + encBody).getBytes(StandardCharsets.UTF_8);
        String encSig = toBase64Url(signingKey.sign(sig));

        // append signature to header and body and return it
        return ENCODED_CLAIM_HEADER + "." + encBody + "." + encSig;
    }
}
