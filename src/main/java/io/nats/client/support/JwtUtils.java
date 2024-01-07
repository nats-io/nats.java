// Copyright 2021-2023 The NATS Authors
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
import io.nats.jwt.Utils;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements <a href="https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-14.md">ADR-14</a>
 */
public abstract class JwtUtils {

    private JwtUtils() {} /* ensures cannot be constructed */

    /**
     * Format string with `%s` placeholder for the JWT token followed
     * by the user NKey seed. This can be directly used as such:
     * 
     * <pre>
     * NKey userKey = NKey.createUser(new SecureRandom());
     * NKey signingKey = loadFromSecretStore();
     * String jwt = issueUserJWT(signingKey, accountId, new String(userKey.getPublicKey()));
     * String.format(Utils.NATS_USER_JWT_FORMAT, jwt, new String(userKey.getSeed()));
     * </pre>
     * @deprecated Use {@link Utils#NATS_USER_JWT_FORMAT} instead.
     */
    @Deprecated
    public static final String NATS_USER_JWT_FORMAT = Utils.NATS_USER_JWT_FORMAT;

    /**
     * Get the current time in seconds since epoch. Used for issue time.
     * @return the time
     */
    public static long currentTimeSeconds() {
        return Utils.currentTimeSeconds();
    }

    /**
     * Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
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
        return issueUserJWT(signingKey, publicUserKey, null, null, currentTimeSeconds(), null, new UserClaim(accountId));
    }

    /**
     * Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
     * @param signingKey a mandatory account nkey pair to sign the generated jwt.
     * @param accountId a mandatory public account nkey. Will throw error when not set or not account nkey.
     * @param publicUserKey a mandatory public user nkey. Will throw error when not set or not user nkey.
     * @param name optional human-readable name. When absent, default to publicUserKey.
     * @throws IllegalArgumentException if the accountId or publicUserKey is not a valid public key of the proper type
     * @throws NullPointerException if signingKey, accountId, or publicUserKey are null.
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException if signingKey sign method throws this exception.
     * @return a JWT
     */
    public static String issueUserJWT(NKey signingKey, String accountId, String publicUserKey, String name) throws GeneralSecurityException, IOException {
        return issueUserJWT(signingKey, publicUserKey, name, null, currentTimeSeconds(), null, new UserClaim(accountId));
    }

    /**
     * Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
     * @param signingKey a mandatory account nkey pair to sign the generated jwt.
     * @param accountId a mandatory public account nkey. Will throw error when not set or not account nkey.
     * @param publicUserKey a mandatory public user nkey. Will throw error when not set or not user nkey.
     * @param name optional human-readable name. When absent, default to publicUserKey.
     * @param expiration optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.
     * @param tags optional list of tags to be included in the JWT.
     * @throws IllegalArgumentException if the accountId or publicUserKey is not a valid public key of the proper type
     * @throws NullPointerException if signingKey, accountId, or publicUserKey are null.
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException if signingKey sign method throws this exception.
     * @return a JWT
     */
    public static String issueUserJWT(NKey signingKey, String accountId, String publicUserKey, String name, Duration expiration, String... tags) throws GeneralSecurityException, IOException {
        return issueUserJWT(signingKey, publicUserKey, name, expiration, currentTimeSeconds(), null, new UserClaim(accountId).tags(tags));
    }

    /**
     * Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
     * @param signingKey a mandatory account nkey pair to sign the generated jwt.
     * @param accountId a mandatory public account nkey. Will throw error when not set or not account nkey.
     * @param publicUserKey a mandatory public user nkey. Will throw error when not set or not user nkey.
     * @param name optional human-readable name. When absent, default to publicUserKey.
     * @param expiration optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.
     * @param tags optional list of tags to be included in the JWT.
     * @param issuedAt the current epoch seconds.
     * @throws IllegalArgumentException if the accountId or publicUserKey is not a valid public key of the proper type
     * @throws NullPointerException if signingKey, accountId, or publicUserKey are null.
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException if signingKey sign method throws this exception.
     * @return a JWT
     */
    public static String issueUserJWT(NKey signingKey, String accountId, String publicUserKey, String name, Duration expiration, String[] tags, long issuedAt) throws GeneralSecurityException, IOException {
        return issueUserJWT(signingKey, publicUserKey, name, expiration, issuedAt, null, new UserClaim(accountId).tags(tags));
    }

    public static String issueUserJWT(NKey signingKey, String accountId, String publicUserKey, String name, Duration expiration, String[] tags, long issuedAt, String audience) throws GeneralSecurityException, IOException {
        return issueUserJWT(signingKey, publicUserKey, name, expiration, issuedAt, audience, new UserClaim(accountId).tags(tags));
    }

    /**
     * Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
     * @param signingKey a mandatory account nkey pair to sign the generated jwt.
     * @param publicUserKey a mandatory public user nkey. Will throw error when not set or not user nkey.
     * @param name optional human-readable name. When absent, default to publicUserKey.
     * @param expiration optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.
     * @param issuedAt the current epoch seconds.
     * @param nats the user claim
     * @throws IllegalArgumentException if the accountId or publicUserKey is not a valid public key of the proper type
     * @throws NullPointerException if signingKey, accountId, or publicUserKey are null.
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException if signingKey sign method throws this exception.
     * @return a JWT
     */
    public static String issueUserJWT(NKey signingKey, String publicUserKey, String name, Duration expiration, long issuedAt, UserClaim nats) throws GeneralSecurityException, IOException {
        return issueUserJWT(signingKey, publicUserKey, name, expiration, issuedAt, null, nats);
    }

    public static String issueUserJWT(NKey signingKey, String publicUserKey, String name, Duration expiration, long issuedAt, io.nats.jwt.UserClaim nats) throws GeneralSecurityException, IOException {
        return issueUserJWT(signingKey, publicUserKey, name, expiration, issuedAt, null, nats);
    }

    /**
     * Issue a user JWT from a scoped signing key. See <a href="https://docs.nats.io/nats-tools/nsc/signing_keys">Signing Keys</a>
     * @param signingKey a mandatory account nkey pair to sign the generated jwt.
     * @param publicUserKey a mandatory public user nkey. Will throw error when not set or not user nkey.
     * @param name optional human-readable name. When absent, default to publicUserKey.
     * @param expiration optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.
     * @param issuedAt the current epoch seconds.
     * @param audience the optional audience
     * @param nats the user claim
     * @throws IllegalArgumentException if the accountId or publicUserKey is not a valid public key of the proper type
     * @throws NullPointerException if signingKey, accountId, or publicUserKey are null.
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException if signingKey sign method throws this exception.
     * @return a JWT
     */
    public static String issueUserJWT(NKey signingKey, String publicUserKey, String name, Duration expiration, long issuedAt, String audience, io.nats.jwt.UserClaim nats) throws GeneralSecurityException, IOException {
        // Validate the signingKey:
        if (signingKey.getType() != NKey.Type.ACCOUNT) {
            throw new IllegalArgumentException("issueUserJWT requires an account key for the signingKey parameter, but got " + signingKey.getType());
        }

        // TODO why are these keys validated but not used?
        // Validate the accountId:
        NKey accountKey = NKey.fromPublicKey(nats.issuerAccount.toCharArray());
        if (accountKey.getType() != NKey.Type.ACCOUNT) {
            throw new IllegalArgumentException("issueUserJWT requires an account key for the accountId parameter, but got " + accountKey.getType());
        }
        // Validate the publicUserKey:
        NKey userKey = NKey.fromPublicKey(publicUserKey.toCharArray());
        if (userKey.getType() != NKey.Type.USER) {
            throw new IllegalArgumentException("issueUserJWT requires a user key for the publicUserKey parameter, but got " + userKey.getType());
        }

        String accSigningKeyPub = new String(signingKey.getPublicKey());

        String claimName = Validator.nullOrEmpty(name) ? publicUserKey : name;

        return issueJWT(signingKey, publicUserKey, claimName, expiration, issuedAt, accSigningKeyPub, audience, nats);
    }

    /**
     * Issue a JWT
     * @param signingKey account nkey pair to sign the generated jwt.
     * @param publicUserKey a mandatory public user nkey.
     * @param name optional human-readable name.
     * @param expiration optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.
     * @param issuedAt the current epoch seconds.
     * @param accSigningKeyPub the account signing key
     * @param nats the generic nats claim
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException if signingKey sign method throws this exception.
     * @return a JWT
     */
    public static String issueJWT(NKey signingKey, String publicUserKey, String name, Duration expiration, long issuedAt, String accSigningKeyPub, JsonSerializable nats) throws GeneralSecurityException, IOException {
        return issueJWT(signingKey, publicUserKey, name, expiration, issuedAt, accSigningKeyPub, null, nats);
    }

    /**
     * Issue a JWT
     *
     * @param signingKey       account nkey pair to sign the generated jwt.
     * @param publicUserKey    a mandatory public user nkey.
     * @param name             optional human-readable name.
     * @param expiration       optional but recommended duration, when the generated jwt needs to expire. If not set, JWT will not expire.
     * @param issuedAt         the current epoch seconds.
     * @param accSigningKeyPub the account signing key
     * @param audience         the optional audience
     * @param nats             the generic nats claim
     * @return a JWT
     * @throws GeneralSecurityException if SHA-256 MessageDigest is missing, or if the signingKey can not be used for signing.
     * @throws IOException              if signingKey sign method throws this exception.
     */
    public static String issueJWT(NKey signingKey, String publicUserKey, String name, Duration expiration, long issuedAt, String accSigningKeyPub, String audience, JsonSerializable nats) throws GeneralSecurityException, IOException {
        return new io.nats.jwt.ClaimIssuer()
            .aud(audience)
            .iat(issuedAt)
            .iss(accSigningKeyPub)
            .name(name)
            .sub(publicUserKey)
            .expiresIn(expiration)
            .nats(nats)
            .issueJwt(signingKey);
    }

    /**
     * Get the claim body from a JWT
     * @param jwt the encoded jwt
     * @return the claim body json
     * @deprecated Use {@link Utils#getClaimBody(String)} instead.
     */
    @Deprecated
    public static String getClaimBody(String jwt) {
        return Utils.getClaimBody(jwt);
    }

    /**
     * @deprecated Use {@link io.nats.jwt.UserClaim} instead.
     */
    @Deprecated
    public static class UserClaim extends io.nats.jwt.UserClaim {
        public UserClaim(String issuerAccount) {
            super(issuerAccount);
        }

        public UserClaim tags(String... tags) {
            super.tags(tags);
            return this;
        }

        public UserClaim pub(Permission pub) {
            super.pub(pub);
            return this;
        }

        public UserClaim sub(Permission sub) {
            super.sub(sub);
            return this;
        }

        public UserClaim resp(ResponsePermission resp) {
            super.resp(resp);
            return this;
        }

        public UserClaim src(String... src) {
            super.src(src);
            return this;
        }

        public UserClaim times(List<TimeRange> times) {
            if (times == null) {
                super.timeRanges(null);
            }
            else {
                super.timeRanges(new ArrayList<>(times));
            }
            return this;
        }

        public UserClaim locale(String locale) {
            super.locale(locale);
            return this;
        }

        public UserClaim subs(long subs) {
            super.subs(subs);
            return this;
        }

        public UserClaim data(long data) {
            super.data(data);
            return this;
        }

        public UserClaim payload(long payload) {
            super.payload(payload);
            return this;
        }

        public UserClaim bearerToken(boolean bearerToken) {
            super.bearerToken(bearerToken);
            return this;
        }

        public UserClaim allowedConnectionTypes(String... allowedConnectionTypes) {
            super.allowedConnectionTypes(allowedConnectionTypes);
            return this;
        }
    }

    public static class TimeRange extends io.nats.jwt.TimeRange {
        public TimeRange(String start, String end) {
            super(start, end);
        }
    }

    public static class ResponsePermission extends io.nats.jwt.ResponsePermission {
        public ResponsePermission maxMsgs(int maxMsgs) {
            super.max(maxMsgs);
            return this;
        }

        public ResponsePermission expires(Duration expires) {
            super.expires(expires);
            return this;
        }

        public ResponsePermission expires(long expiresMillis) {
            super.expires(expiresMillis);
            return this;
        }
    }

    public static class Permission extends io.nats.jwt.Permission {
        public Permission allow(String... allow) {
            super.allow(allow);
            return this;
        }

        public Permission deny(String... deny) {
            super.deny(deny);
            return this;
        }
    }
}
