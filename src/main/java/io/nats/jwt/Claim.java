// Copyright 2021-2024 The NATS Authors
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

import io.nats.client.support.*;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.jwt.Utils.*;

public class Claim implements JsonSerializable {
    public final String aud;
    public final long exp;
    public final String jti;
    public final long iat;
    public final String iss;
    public final String name;
    public final String nbf;
    public final String sub;
    public final String type;
    public final JsonSerializable nats;
    public final UserClaim userClaim;
    public final AuthorizationRequest authorizationRequest;
    public final AuthorizationResponse authorizationResponse;

    public Claim(String json) throws JsonParseException {
        this(JsonParser.parse(json));
    }

    public Claim(byte[] json) throws JsonParseException {
        this(JsonParser.parse(json));
    }

    public Claim(char[] json) throws JsonParseException {
        this(JsonParser.parse(json));
    }

    Claim(ClaimIssuer issuer) {
        aud = issuer.aud;
        exp = issuer.exp == null ? 0 : issuer.exp;
        jti = issuer.jti;
        iat = issuer.iat;
        iss = issuer.iss;
        name = issuer.name;
        nbf = issuer.nbf;
        sub = issuer.sub;
        nats = issuer.nats;
        UserClaim tempUserClaim = null;
        AuthorizationRequest tempAuthorizationRequest = null;
        AuthorizationResponse tempAuthorizationResponse = null;

        if (nats instanceof UserClaim) {
            tempUserClaim = (UserClaim)nats;
            type = tempUserClaim.getType();
        }
        else if (nats instanceof AuthorizationRequest) {
            tempAuthorizationRequest = (AuthorizationRequest) nats;
            type = tempAuthorizationRequest.getType();
        }
        else if (nats instanceof AuthorizationResponse) {
            tempAuthorizationResponse = (AuthorizationResponse) nats;
            type = tempAuthorizationResponse.getType();
        }
        else {
            type = JsonValueUtils.readString(nats.toJsonValue(), "type");
        }
        userClaim = tempUserClaim;
        authorizationRequest = tempAuthorizationRequest;
        authorizationResponse = tempAuthorizationResponse;
    }

    public Claim(JsonValue jv) {
        aud = JsonValueUtils.readString(jv, "aud");
        exp = JsonValueUtils.readLong(jv, "exp", -1);
        jti = JsonValueUtils.readString(jv, "jti");
        iat = JsonValueUtils.readLong(jv, "iat", -1);
        iss = JsonValueUtils.readString(jv, "iss");
        name = JsonValueUtils.readString(jv, "name");
        nbf = JsonValueUtils.readString(jv, "nbf");
        sub = JsonValueUtils.readString(jv, "sub");

        JsonValue nats = JsonValueUtils.readValue(jv, "nats");
        this.nats = nats;

        type = JsonValueUtils.readString(nats, "type");
        if (USER_CLAIM_TYPE.equals(type)) {
            userClaim = new UserClaim(nats);
            authorizationRequest = null;
            authorizationResponse = null;
        }
        else if (AUTH_REQUEST_CLAIM_TYPE.equals(type)) {
            userClaim = null;
            authorizationRequest = new AuthorizationRequest(nats);
            authorizationResponse = null;
        }
        else if (AUTH_RESPONSE_CLAIM_TYPE.equals(type)) {
            userClaim = null;
            authorizationRequest = null;
            authorizationResponse = new AuthorizationResponse(nats);
        }
        else {
            userClaim = null;
            authorizationRequest = null;
            authorizationResponse = null;
        }
    }

    @Override
    public String toJson() {
        return toJson(this, jti);
    }

    public static String toJson(Claim c, String jti) {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, "aud", c.aud);
        JsonUtils.addFieldEvenEmpty(sb, "jti", jti);
        JsonUtils.addField(sb, "iat", c.iat);
        JsonUtils.addField(sb, "iss", c.iss);
        JsonUtils.addField(sb, "name", c.name);
        JsonUtils.addField(sb, "sub", c.sub);
        JsonUtils.addFieldWhenGtZero(sb, "exp", c.exp);
        JsonUtils.addField(sb, "nbf", c.nbf);
        JsonUtils.addField(sb, "nats", c.nats);
        return endJson(sb).toString();
    }
}
