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

package io.nats.jwt;

import static io.nats.client.support.Encoding.fromBase64Url;
import static io.nats.client.support.Encoding.toBase64Url;

public abstract class Utils {
    public static final String USER_CLAIM_TYPE = "user";
    public static final String AUTH_REQUEST_CLAIM_TYPE = "authorization_request";
    public static final String AUTH_RESPONSE_CLAIM_TYPE = "authorization_response";
    public static final String ENCODED_CLAIM_HEADER = toBase64Url("{\"typ\":\"JWT\", \"alg\":\"ed25519-nkey\"}");
    public static final long NO_LIMIT = -1;

    private Utils() {} /* ensures cannot be constructed */

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
     */
    public static final String NATS_USER_JWT_FORMAT = "-----BEGIN NATS USER JWT-----\n" +
            "%s\n" +
            "------END NATS USER JWT------\n" +
            "\n" +
            "************************* IMPORTANT *************************\n" +
            "NKEY Seed printed below can be used to sign and prove identity.\n" +
            "NKEYs are sensitive and should be treated as secrets.\n" +
            "\n" +
            "-----BEGIN USER NKEY SEED-----\n" +
            "%s\n" +
            "------END USER NKEY SEED------\n" +
            "\n" +
            "*************************************************************\n";

    /**
     * Get the current time in seconds since epoch. Used for issue time.
     * @return the time
     */
    public static long currentTimeSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * Get the claim body from a JWT
     * @param jwt the encoded jwt string
     * @return the claim body json
     */
    public static String getClaimBody(String jwt) {
        return fromBase64Url(jwt.split("\\.")[1]);
    }

    /**
     * Get the claim body from a JWT
     * @param jwtBytes the encoded jwt bytes
     * @return the claim body json
     */
    public static String getClaimBody(byte[] jwtBytes) {
        return getClaimBody(new String(jwtBytes));
    }
}
